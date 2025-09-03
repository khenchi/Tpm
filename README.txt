#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Access → Excel Exporter (GUI) + METADATA
- Sélection multi-fichiers Access (.accdb/.mdb)
- Exporte toutes les tables ET requêtes SELECT en feuilles Excel
- 1 fichier Excel par base, même nom, même dossier
- Ajoute une feuille METADATA : object_name, object_type, rows_exported, last_update, export_time_utc
- GUI Tkinter avec logs temps réel, export en thread

Prérequis :
    pip install pyodbc pandas XlsxWriter
    (Installer le Microsoft Access Database Engine, même architecture que Python)
"""

from __future__ import annotations

import os
import sys
import threading
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import tkinter as tk
from tkinter import filedialog, messagebox

import pyodbc
import pandas as pd
import subprocess


# -----------------------------
# Utilitaires "clean"
# -----------------------------

INVALID_SHEET_CHARS = set(r'[]:*?/\\')
MAX_SHEET_LEN = 31


def sanitize_sheet_name(name: str) -> str:
    """Nettoie un nom de feuille Excel (caractères interdits, longueur, quotes)."""
    cleaned = "".join(ch for ch in name if ch not in INVALID_SHEET_CHARS)
    cleaned = cleaned.strip().strip("'")
    if not cleaned:
        cleaned = "Sheet"
    if len(cleaned) > MAX_SHEET_LEN:
        cleaned = cleaned[:MAX_SHEET_LEN]
    return cleaned


def ensure_unique_sheet_name(base: str, existing: set) -> str:
    """Évite les doublons de feuilles en suffixant (2), (3), ... au besoin."""
    name = sanitize_sheet_name(base)
    if name not in existing:
        existing.add(name)
        return name
    stem = name[:28].rstrip()  # réserve pour " (99)"
    i = 2
    while True:
        candidate = f"{stem} ({i})"
        candidate = candidate[:MAX_SHEET_LEN]
        if candidate not in existing:
            existing.add(candidate)
            return candidate
        i += 1


@dataclass(frozen=True)
class DbObject:
    name: str
    type: str  # "TABLE" ou "VIEW" (les requêtes SELECT apparaissent comme VIEW via ODBC)


# -----------------------------
# Cœur métier
# -----------------------------

class AccessExporter:
    """Logique d'accès à Access et export vers Excel."""

    def __init__(self, log_fn):
        """
        :param log_fn: fonction de logging (str) -> None, fournie par la GUI.
        """
        self.log = log_fn

    def connect(self, mdb_path: Path) -> pyodbc.Connection:
        """Ouvre une connexion ODBC Access."""
        conn_str = (
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            rf"Dbq={mdb_path};"
        )
        try:
            return pyodbc.connect(conn_str, autocommit=False)
        except pyodbc.Error as e:
            raise RuntimeError(
                "Impossible de se connecter à la base. "
                "Vérifie que le 'Microsoft Access Database Engine' est installé "
                "et que l’architecture (32/64 bits) correspond à ton Python.\n"
                f"Détails ODBC: {e}"
            ) from e

    def list_objects(self, cn: pyodbc.Connection) -> List[DbObject]:
        """
        Récupère la liste des objets exportables.
        - Tables utilisateur => TABLE
        - Requêtes SELECT => VIEW (Access les expose comme VIEW via ODBC)
        Filtre les objets systèmes (MSys*).
        """
        objs: List[DbObject] = []
        cursor = cn.cursor()

        for row in cursor.tables():
            name = row.table_name
            ttype = (row.table_type or "").upper()
            if not name or name.startswith("MSys"):
                continue  # ignorer tables système
            if ttype in {"TABLE", "VIEW"}:
                objs.append(DbObject(name=name, type=ttype))

        # Déduplication prudente (certains pilotes peuvent dupliquer)
        uniq: Dict[Tuple[str, str], DbObject] = {(o.name, o.type): o for o in objs}
        return list(uniq.values())

    def fetch_last_updates(self, cn: pyodbc.Connection) -> Dict[str, Optional[pd.Timestamp]]:
        """
        Map {NomObjet -> TimestampUTC approx} depuis MSysObjects.DateUpdate.
        Retourne {} si non accessible (droits/paramètres).
        Codes Type (indicatifs) : 1/4 tables, 5 requêtes.
        """
        try:
            sql = """
                SELECT Name, Type, DateUpdate
                FROM MSysObjects
                WHERE Name NOT LIKE 'MSys%%'
                  AND Type IN (1,4,5)
            """
            df = pd.read_sql(sql, cn)
            if "DateUpdate" in df.columns:
                df["DateUpdate"] = pd.to_datetime(df["DateUpdate"], errors="coerce", utc=True)
            return {str(row["Name"]): row.get("DateUpdate") for _, row in df.iterrows()}
        except Exception:
            self.log("   ℹ️ Impossible de lire MSysObjects.DateUpdate (droits ODBC ?). La colonne 'last_update' sera vide.")
            return {}

    def fetch_frame(self, cn: pyodbc.Connection, obj: DbObject, max_rows: Optional[int] = None) -> pd.DataFrame:
        """
        Charge un objet Access dans un DataFrame.
        - Tables et vues (requêtes SELECT)
        - Utilise SELECT * FROM [name], éventuellement TOP n
        """
        name_escaped = f"[{obj.name}]"
        if max_rows is not None and max_rows > 0:
            sql = f"SELECT TOP {int(max_rows)} * FROM {name_escaped}"
        else:
            sql = f"SELECT * FROM {name_escaped}"
        try:
            df = pd.read_sql(sql, cn)
            return df
        except Exception as e:
            raise RuntimeError(f"Échec lecture '{obj.name}' ({obj.type}): {e}") from e

    def export_db_to_excel(self, mdb_path: Path) -> Path:
        """
        Exporte toutes les tables + vues (requêtes SELECT) d'une base Access
        dans un fichier Excel (.xlsx) portant le même nom que la base.
        Ajoute une feuille METADATA.
        """
        self.log(f"— Connexion: {mdb_path}")
        with self.connect(mdb_path) as cn:
            objects = self.list_objects(cn)
            out_path = mdb_path.with_suffix(".xlsx")

            # Lecture des DateUpdate (si possible)
            last_updates = self.fetch_last_updates(cn)

            sheet_names: set = set()
            exported = 0
            skipped: List[str] = []

            # Lignes METADATA
            meta_rows: List[Dict[str, object]] = []
            export_ts = pd.Timestamp.utcnow()

            with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
                for obj in objects:
                    try:
                        self.log(f"   • Lecture {obj.type:>5} : {obj.name}")
                        df = self.fetch_frame(cn, obj)

                        sheet = ensure_unique_sheet_name(obj.name, sheet_names)
                        df.to_excel(writer, sheet_name=sheet, index=False)
                        exported += 1

                        meta_rows.append({
                            "object_name": obj.name,
                            "object_type": "TABLE" if obj.type == "TABLE" else "QUERY",
                            "rows_exported": int(df.shape[0]),
                            "last_update": last_updates.get(obj.name),   # pandas.Timestamp | None
                            "export_time_utc": export_ts,
                        })
                    except Exception as e:
                        self.log(f"     ⚠️ Skippé '{obj.name}' — {e}")
                        skipped.append(obj.name)

                # Feuille METADATA (toujours écrite)
                meta_df = pd.DataFrame(meta_rows, columns=[
                    "object_name", "object_type", "rows_exported", "last_update", "export_time_utc"
                ])
                meta_sheet = ensure_unique_sheet_name("METADATA", sheet_names)
                meta_df.to_excel(writer, sheet_name=meta_sheet, index=False)

            self.log(f"   ✔ Export terminé : {exported} feuille(s) écrite(s)")
            if skipped:
                self.log(f"   ⚠️ Objets non exportés ({len(skipped)}): {', '.join(skipped)}")
            self.log(f"   ✔ Fichier créé : {out_path}")

            return out_path


# -----------------------------
# GUI Tkinter (thread-safe)
# -----------------------------

class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Access → Excel Exporter")
        self.geometry("820x560")
        self.resizable(True, True)

        # Widgets
        self.btn_select = tk.Button(self, text="Sélectionner bases Access…", command=self.on_select_files)
        self.btn_run = tk.Button(self, text="Exporter", command=self.on_export, state=tk.DISABLED)
        self.chk_open_var = tk.BooleanVar(value=True)
        self.chk_open = tk.Checkbutton(self, text="Ouvrir le dossier après export", variable=self.chk_open_var)

        self.files_list = tk.Listbox(self, selectmode=tk.EXTENDED, height=8)
        self.scroll_files = tk.Scrollbar(self, orient="vertical", command=self.files_list.yview)
        self.files_list.config(yscrollcommand=self.scroll_files.set)

        self.log_text = tk.Text(self, height=18, wrap="word", state=tk.DISABLED)
        self.scroll_log = tk.Scrollbar(self, orient="vertical", command=self.log_text.yview)
        self.log_text.config(yscrollcommand=self.scroll_log.set)

        # Layout (grid)
        self.btn_select.grid(row=0, column=0, padx=10, pady=10, sticky="w")
        self.btn_run.grid(row=0, column=1, padx=10, pady=10, sticky="w")
        self.chk_open.grid(row=0, column=2, padx=10, pady=10, sticky="w")

        self.files_list.grid(row=1, column=0, columnspan=3, padx=(10,0), pady=(0,10), sticky="nsew")
        self.scroll_files.grid(row=1, column=3, padx=(0,10), pady=(0,10), sticky="ns")

        self.log_text.grid(row=2, column=0, columnspan=3, padx=(10,0), pady=(0,10), sticky="nsew")
        self.scroll_log.grid(row=2, column=3, padx=(0,10), pady=(0,10), sticky="ns")

        # Grid weights
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=0)
        self.grid_columnconfigure(2, weight=0)
        self.grid_columnconfigure(3, weight=0)
        self.grid_rowconfigure(1, weight=1)
        self.grid_rowconfigure(2, weight=2)

        self.selected_files: List[Path] = []
        self.exporter = AccessExporter(log_fn=self.log)

        # State
        self._worker: Optional[threading.Thread] = None

    # ----- Logging thread-safe -----

    def log(self, msg: str) -> None:
        def _append():
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, msg + "\n")
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)
        self.after(0, _append)

    # ----- Actions -----

    def on_select_files(self) -> None:
        filetypes = [
            ("Fichiers Access", "*.accdb *.mdb"),
            ("Access 2007+", "*.accdb"),
            ("Access (legacy)", "*.mdb"),
            ("Tous les fichiers", "*.*"),
        ]
        paths = filedialog.askopenfilenames(
            title="Choisir une ou plusieurs bases Access",
            filetypes=filetypes
        )
        if not paths:
            return

        self.selected_files = [Path(p) for p in paths]
        self.files_list.delete(0, tk.END)
        for p in self.selected_files:
            self.files_list.insert(tk.END, str(p))
        self.btn_run.config(state=tk.NORMAL)

    def on_export(self) -> None:
        if self._worker and self._worker.is_alive():
            messagebox.showinfo("Export en cours", "Un export est déjà en cours…")
            return

        if not self.selected_files:
            messagebox.showwarning("Aucun fichier", "Sélectionne d’abord au moins une base Access.")
            return

        self.btn_run.config(state=tk.DISABLED)
        self.log("=== Début export ===")
        self._worker = threading.Thread(target=self._export_worker, daemon=True)
        self._worker.start()

    def _open_folder(self, path: Path) -> None:
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception:
            pass

    def _export_worker(self) -> None:
        try:
            outputs: List[Path] = []
            for mdb in self.selected_files:
                try:
                    out = self.exporter.export_db_to_excel(mdb)
                    outputs.append(out)
                except Exception as e:
                    self.log(f"⛔ Erreur sur {mdb.name} : {e}")
                    self.log(traceback.format_exc())

            self.log("=== Fin export ===")
            if outputs and self.chk_open_var.get():
                last_dir = outputs[-1].parent
                self._open_folder(last_dir)
        finally:
            self.after(0, lambda: self.btn_run.config(state=tk.NORMAL))


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
