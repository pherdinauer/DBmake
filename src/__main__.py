"""
Entry point for the ANAC Data Importer package
"""
from import_json import import_all_json_files

if __name__ == "__main__":
    import_all_json_files("/database/JSON", "database.db") 