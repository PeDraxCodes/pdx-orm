import argparse
import ast
import sys
from pathlib import Path
from typing import List, Optional, Set

# Annahme: Deine Basisklasse heißt 'BaseData'
# Du kannst dies anpassen, falls sie anders heißt.
BASE_CLASS_NAME = "BaseData"


class InfoFieldDetails:
    """Hilfsklasse zum Speichern extrahierter Infos aus InfoField."""

    def __init__(self):
        self.default_value_repr: Optional[str] = None
        self.is_nullable_in_infofield: bool = False  # Falls InfoField auch 'nullable' hat
        # Du könntest hier weitere Infos speichern, falls nötig


def get_infofield_details(node: ast.Call) -> InfoFieldDetails:
    """Extrahiert Details aus dem ast.Call-Knoten von InfoField."""
    details = InfoFieldDetails()
    if not isinstance(node.func, ast.Name) or node.func.id != 'InfoField':
        return details  # Nicht der erwartete InfoField-Aufruf

    # Extrahiere 'nullable'-Argument (Position 1, falls vorhanden)
    if len(node.args) > 1:
        nullable_arg = node.args[1]
        if isinstance(nullable_arg, ast.Constant):
            details.is_nullable_in_infofield = bool(nullable_arg.value)

    # Extrahiere 'default_value' aus Keyword-Argumenten
    for kw in node.keywords:
        if kw.arg == 'default_value':
            # Wir brauchen die String-Repräsentation des Werts
            try:
                details.default_value_repr = ast.unparse(kw.value).strip()
            except AttributeError:  # Fallback für sehr alte ast-Versionen
                # Dies ist sehr vereinfacht und nicht robust für komplexe Defaults
                if isinstance(kw.value, ast.Constant):
                    details.default_value_repr = repr(kw.value.value)
            break
    return details


def is_optional_type(type_str: str) -> bool:
    """Prüft, ob ein Typ-String 'Optional' oder '| None' enthält."""
    return type_str.startswith("Optional[") or \
        type_str.endswith("| None") or \
        type_str.strip() == "None"


class StubVisitor(ast.NodeVisitor):
    """
    Besucht AST-Knoten, um Klassendefinitionen zu finden
    und Stub-Informationen zu sammeln.
    """

    def __init__(self, base_class_name: str):
        self.base_class_name = base_class_name
        self.stub_parts: List[str] = []
        self.imports: Set[str] = set(["from typing import Optional, Any"])  # Standard-Imports

    def _format_type_hint(self, node: Optional[ast.expr]) -> str:
        """Formatiert einen Typ-Hint-AST-Knoten als String."""
        if node is None:
            return "Any"  # Fallback, sollte nicht passieren bei AnnAssign
        try:
            type_str = ast.unparse(node).strip()
            # Sammle potenzielle Imports (vereinfacht)
            if "Optional[" in type_str or "| None" in type_str:
                self.imports.add("from typing import Optional")
            # Man könnte hier weiter gehen und alle Namen sammeln und
            # prüfen, ob sie Builtins sind oder importiert werden müssen.
            return type_str
        except AttributeError:
            # Fallback für ältere Python-Versionen
            return "Any"  # Oder eine komplexere manuelle Rekonstruktion

    def visit_ClassDef(self, node: ast.ClassDef):
        # Prüfen, ob die Klasse von der gewünschten Basisklasse erbt
        inherits_from_base = False
        base_names = []
        for base in node.bases:
            base_name = self._format_type_hint(base)  # Formatierung wiederverwenden
            base_names.append(base_name)
            if base_name == self.base_class_name:
                inherits_from_base = True

        if not inherits_from_base:
            self.generic_visit(node)  # Besuche Kinder, falls es verschachtelte Klassen gibt
            return  # Überspringe Klassen, die nicht erben

        class_name = node.name
        base_str = f"({', '.join(base_names)})" if base_names else ""

        fields: List[str] = []
        init_params: List[str] = []

        # Sammle Felder und Init-Parameter aus AnnAssign-Knoten
        for item in node.body:
            if isinstance(item, ast.AnnAssign):
                field_name = item.target.id
                type_hint_str = self._format_type_hint(item.annotation)

                # Felddefinition für den Stub-Body
                fields.append(f"    {field_name}: {type_hint_str}")

                # Standardwert für __init__ ermitteln
                default_repr: Optional[str] = None
                is_optional = is_optional_type(type_hint_str)

                if isinstance(item.value, ast.Call):
                    infofield_details = get_infofield_details(item.value)
                    if infofield_details.default_value_repr is not None:
                        default_repr = infofield_details.default_value_repr
                    elif is_optional:  # Kein expliziter Default, aber optionaler Typ
                        default_repr = "None"
                    # Sonst: Kein Default -> Pflichtparameter

                # Parameter für __init__ formatieren
                if default_repr is not None:
                    init_params.append(f"{field_name}: {type_hint_str} = {default_repr}")
                else:
                    init_params.append(f"{field_name}: {type_hint_str}")

        # Stub-Teil für diese Klasse zusammenbauen
        class_stub = f"\n\nclass {class_name}{base_str}:\n"
        if fields:
            class_stub += "\n".join(fields) + "\n"
        else:
            class_stub += "    pass\n"  # Falls keine Felder gefunden wurden

        # __init__ hinzufügen
        if init_params:
            init_sig = "    def __init__(\n        self,\n        *,\n"  # Erzwingt Keyword-Argumente
            init_sig += ",\n".join([f"        {p}" for p in init_params])
            init_sig += "\n    ) -> None: ..."
        else:
            init_sig = "    def __init__(self) -> None: ..."  # Einfacher Init wenn keine Felder

        class_stub += "\n" + init_sig + "\n"

        self.stub_parts.append(class_stub)
        # Besuche keine Kinder von passenden Klassen mehr, da wir alles verarbeitet haben
        # self.generic_visit(node) # Auskommentiert


def generate_stub_file(source_file: Path, target_file: Path, base_class_name: str):
    """Liest eine Python-Datei, parst sie und schreibt die Stub-Datei."""
    print(f"Processing {source_file}...")
    try:
        source_code = source_file.read_text(encoding='utf-8')
        tree = ast.parse(source_code)
    except FileNotFoundError:
        print(f"Error: Source file not found: {source_file}", file=sys.stderr)
        sys.exit(1)
    except SyntaxError as e:
        print(f"Error: Could not parse Python file {source_file}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading or parsing file {source_file}: {e}", file=sys.stderr)
        sys.exit(1)

    visitor = StubVisitor(base_class_name)
    visitor.visit(tree)

    if not visitor.stub_parts:
        print(f"Warning: No classes inheriting from '{base_class_name}' found in {source_file}.")
        # Entscheide, ob trotzdem eine leere Stub-Datei erstellt werden soll
        # return

    # Header und Imports hinzufügen
    output_content = "# pylint: skip-file\n"  # Hinweis für Pylint
    output_content += "# -*- coding: utf-8 -*-\n"
    output_content += "\"\"\"Auto-generated stub file for {} - DO NOT EDIT\"\"\"\n\n".format(source_file.name)
    output_content += "\n".join(sorted(list(visitor.imports)))
    output_content += "".join(visitor.stub_parts)

    try:
        target_file.parent.mkdir(parents=True, exist_ok=True)
        target_file.write_text(output_content, encoding='utf-8')
        print(f"Successfully generated stub file: {target_file}")
    except IOError as e:
        print(f"Error: Could not write stub file {target_file}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during file writing: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate .pyi stub files for Python classes inheriting from a specific base class.")
    parser.add_argument("source_file", type=Path, help="Path to the source Python file (.py).")
    parser.add_argument("-o", "--output", type=Path, default=None,
                        help="Path to the output stub file (.pyi). Defaults to the same directory and name as the source file with a .pyi extension.")
    parser.add_argument("-b", "--base-class", type=str, default=BASE_CLASS_NAME,
                        help=f"Name of the base class to look for (default: {BASE_CLASS_NAME}).")

    args = parser.parse_args()

    source_path: Path = args.source_file
    output_path: Path = args.output

    if output_path is None:
        # Standard-Ausgabepfad: Gleicher Name, aber .pyi Endung
        output_path = source_path.with_suffix(".pyi")

    if source_path == output_path:
        print(f"Error: Source and target file cannot be the same: {source_path}", file=sys.stderr)
        sys.exit(1)

    generate_stub_file(source_path, output_path, args.base_class)
