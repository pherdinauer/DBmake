#!/usr/bin/env python3
"""
Wizard CLI interattivo per ANAC Importer Enterprise Edition.

Guida l'utente attraverso:
- Configurazione credenziali database
- Creazione database automatica
- Inizializzazione schema enterprise
- Test connettività e integrità
- Setup completo sistema
"""

import sys
import os
import time
import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import logging

# Aggiungi src al path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import click
    from rich.console import Console
    from rich.prompt import Prompt, Confirm
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.text import Text
    from rich import box
except ImportError:
    print("🚨 Dipendenze mancanti. Installa: pip install click rich")
    sys.exit(1)

try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:
    print("🚨 MySQL connector mancante. Installa: pip install mysql-connector-python")
    sys.exit(1)

# Import dei moduli enterprise
try:
    from src.security import SecureCredentialManager
    from src.database.secure_connection import SecureDatabaseConnection
    from src.database.schema_manager import SchemaManager
except ImportError as e:
    print(f"🚨 Moduli enterprise non disponibili: {e}")
    # Continua comunque per il wizard base

console = Console()

class SetupWizard:
    """
    Wizard interattivo per setup completo ANAC Importer Enterprise.
    
    Funzionalità:
    - Configurazione guidata credenziali
    - Test connessione database
    - Creazione database automatica
    - Inizializzazione schema enterprise
    - Validazione setup completo
    """
    
    def __init__(self):
        self.console = Console()
        self.credentials = {}
        self.db_connection = None
        self.schema_manager = None
        
    def run_wizard(self) -> bool:
        """Esegue il wizard completo di setup."""
        try:
            self._show_welcome()
            
            # Step 1: Raccolta credenziali
            if not self._collect_credentials():
                return False
            
            # Step 2: Test connessione
            if not self._test_connection():
                return False
            
            # Step 3: Setup database
            if not self._setup_database():
                return False
            
            # Step 4: Inizializzazione schema
            if not self._initialize_schema():
                return False
            
            # Step 5: Salvataggio credenziali
            if not self._save_credentials():
                return False
            
            # Step 6: Test finale
            if not self._final_validation():
                return False
            
            self._show_success()
            return True
            
        except KeyboardInterrupt:
            self.console.print("\n❌ Setup interrotto dall'utente", style="red")
            return False
        except Exception as e:
            self.console.print(f"\n🚨 Errore durante setup: {e}", style="red")
            return False
    
    def _show_welcome(self):
        """Mostra il messaggio di benvenuto."""
        welcome_text = """
🚀 ANAC Importer Enterprise Edition
Setup Wizard Interattivo

Questo wizard ti guiderà attraverso la configurazione completa del sistema:
• Configurazione credenziali database sicure
• Test connettività e permessi
• Creazione database automatica (se necessario)
• Inizializzazione schema enterprise
• Validazione setup completo

Premi Ctrl+C per uscire in qualsiasi momento.
        """
        
        panel = Panel(
            welcome_text.strip(),
            title="🔧 Setup Wizard",
            title_align="left",
            border_style="blue",
            box=box.ROUNDED
        )
        
        self.console.print(panel)
        self.console.print()
        
        if not Confirm.ask("Vuoi procedere con il setup?", default=True):
            raise KeyboardInterrupt("Setup annullato dall'utente")
    
    def _collect_credentials(self) -> bool:
        """Raccoglie le credenziali del database in modo guidato."""
        self.console.print("📋 [bold blue]STEP 1: Configurazione Credenziali Database[/bold blue]")
        self.console.print()
        
        # Host MySQL
        while True:
            host = Prompt.ask(
                "🌐 Host MySQL",
                default="localhost",
                show_default=True
            )
            if self._validate_host(host):
                self.credentials['host'] = host
                break
            self.console.print("❌ Host non valido. Riprova.", style="red")
        
        # Porta MySQL
        while True:
            port_str = Prompt.ask(
                "🔌 Porta MySQL",
                default="3306",
                show_default=True
            )
            try:
                port = int(port_str)
                if 1 <= port <= 65535:
                    self.credentials['port'] = port
                    break
            except ValueError:
                pass
            self.console.print("❌ Porta non valida (1-65535). Riprova.", style="red")
        
        # Username
        while True:
            user = Prompt.ask("👤 Username MySQL")
            if user and len(user) > 0:
                self.credentials['user'] = user
                break
            self.console.print("❌ Username obbligatorio. Riprova.", style="red")
        
        # Password
        while True:
            password = Prompt.ask("🔑 Password MySQL", password=True)
            if self._validate_password(password):
                self.credentials['password'] = password
                break
        
        # Nome database
        while True:
            database = Prompt.ask(
                "🗄️ Nome database ANAC",
                default="anac_enterprise",
                show_default=True
            )
            if self._validate_database_name(database):
                self.credentials['database'] = database
                break
            self.console.print("❌ Nome database non valido. Riprova.", style="red")
        
        # SSL
        ssl_enabled = Confirm.ask(
            "🔒 Abilitare SSL per connessioni sicure?",
            default=True
        )
        self.credentials['ssl_disabled'] = not ssl_enabled
        
        # Mostra riepilogo
        self._show_credentials_summary()
        
        return Confirm.ask("✅ Confermi le credenziali?", default=True)
    
    def _validate_host(self, host: str) -> bool:
        """Valida l'host MySQL."""
        if not host or len(host.strip()) == 0:
            return False
        
        # Controlli base
        if len(host) > 255:
            return False
        
        # Potrebbe essere IP, hostname, o localhost
        return True
    
    def _validate_password(self, password: str) -> bool:
        """Valida la password con regole di sicurezza."""
        if not password:
            self.console.print("❌ Password obbligatoria.", style="red")
            return False
        
        if len(password) < 8:
            self.console.print("❌ Password troppo corta (minimo 8 caratteri).", style="red")
            return False
        
        # Password comuni da evitare
        weak_passwords = [
            'password', '12345678', 'admin', 'root', 'mysql',
            'DataBase2025!', 'password123', 'admin123'
        ]
        
        if password.lower() in [p.lower() for p in weak_passwords]:
            self.console.print("❌ Password troppo comune. Scegli una password più sicura.", style="red")
            return False
        
        self.console.print("✅ Password sicura", style="green")
        return True
    
    def _validate_database_name(self, name: str) -> bool:
        """Valida il nome del database."""
        if not name or len(name.strip()) == 0:
            return False
        
        # Solo caratteri alfanumerici e underscore
        import re
        if not re.match(r'^[a-zA-Z0-9_]+$', name):
            return False
        
        if len(name) > 64:  # Limite MySQL
            return False
        
        return True
    
    def _show_credentials_summary(self):
        """Mostra un riepilogo delle credenziali inserite."""
        table = Table(title="📋 Riepilogo Credenziali", box=box.ROUNDED)
        table.add_column("Parametro", style="cyan")
        table.add_column("Valore", style="white")
        
        table.add_row("Host", self.credentials['host'])
        table.add_row("Porta", str(self.credentials['port']))
        table.add_row("Username", self.credentials['user'])
        table.add_row("Password", "••••••••")
        table.add_row("Database", self.credentials['database'])
        table.add_row("SSL", "Abilitato" if not self.credentials['ssl_disabled'] else "Disabilitato")
        
        self.console.print()
        self.console.print(table)
        self.console.print()
    
    def _test_connection(self) -> bool:
        """Testa la connessione al database."""
        self.console.print("🔗 [bold blue]STEP 2: Test Connessione Database[/bold blue]")
        self.console.print()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            
            # Test connessione al server MySQL
            task1 = progress.add_task("🌐 Connessione al server MySQL...", total=None)
            
            try:
                # Connessione senza specificare il database
                test_config = self.credentials.copy()
                del test_config['database']  # Rimuovi database per test server
                
                connection = mysql.connector.connect(**test_config)
                progress.update(task1, description="✅ Connesso al server MySQL")
                time.sleep(0.5)
                
                # Test permessi
                task2 = progress.add_task("🔑 Verifica permessi utente...", total=None)
                
                cursor = connection.cursor()
                
                # Verifica permessi di creazione database
                cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
                grants = cursor.fetchall()
                
                has_create_permission = any(
                    'CREATE' in str(grant) or 'ALL PRIVILEGES' in str(grant)
                    for grant in grants
                )
                
                if has_create_permission:
                    progress.update(task2, description="✅ Permessi sufficienti")
                else:
                    progress.update(task2, description="⚠️ Permessi limitati")
                    self.console.print("⚠️ L'utente potrebbe non avere permessi per creare database", style="yellow")
                
                cursor.close()
                connection.close()
                
                time.sleep(0.5)
                
            except Error as e:
                progress.update(task1, description="❌ Connessione fallita")
                self.console.print(f"\n🚨 Errore connessione: {e}", style="red")
                
                if "Access denied" in str(e):
                    self.console.print("💡 Suggerimento: Verifica username e password", style="yellow")
                elif "Can't connect" in str(e):
                    self.console.print("💡 Suggerimento: Verifica host e porta", style="yellow")
                
                return Confirm.ask("Vuoi riprovare con credenziali diverse?", default=True) and self._collect_credentials()
        
        self.console.print("✅ [green]Connessione al server MySQL riuscita![/green]")
        return True
    
    def _setup_database(self) -> bool:
        """Setup del database (creazione se necessario)."""
        self.console.print("\n🗄️ [bold blue]STEP 3: Setup Database[/bold blue]")
        self.console.print()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            
            # Connessione senza database
            task1 = progress.add_task("🔍 Verifica esistenza database...", total=None)
            
            try:
                test_config = self.credentials.copy()
                del test_config['database']
                
                connection = mysql.connector.connect(**test_config)
                cursor = connection.cursor()
                
                # Verifica se il database esiste
                cursor.execute("SHOW DATABASES")
                databases = [db[0] for db in cursor.fetchall()]
                
                database_exists = self.credentials['database'] in databases
                
                if database_exists:
                    progress.update(task1, description="✅ Database esistente trovato")
                    self.console.print(f"✅ Database '{self.credentials['database']}' già esistente")
                    
                    # Chiedi se usare quello esistente
                    use_existing = Confirm.ask(
                        f"Vuoi usare il database esistente '{self.credentials['database']}'?",
                        default=True
                    )
                    
                    if not use_existing:
                        # Chiedi nuovo nome
                        new_name = Prompt.ask("Inserisci nuovo nome database")
                        if self._validate_database_name(new_name):
                            self.credentials['database'] = new_name
                            database_exists = new_name in databases
                
                if not database_exists:
                    # Crea il database
                    task2 = progress.add_task("🏗️ Creazione database...", total=None)
                    
                    create_query = f"CREATE DATABASE `{self.credentials['database']}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                    cursor.execute(create_query)
                    
                    progress.update(task2, description="✅ Database creato")
                    self.console.print(f"✅ Database '{self.credentials['database']}' creato con successo!")
                
                cursor.close()
                connection.close()
                
            except Error as e:
                progress.update(task1, description="❌ Errore setup database")
                self.console.print(f"\n🚨 Errore setup database: {e}", style="red")
                return False
        
        return True
    
    def _initialize_schema(self) -> bool:
        """Inizializza lo schema enterprise."""
        self.console.print("\n🏗️ [bold blue]STEP 4: Inizializzazione Schema Enterprise[/bold blue]")
        self.console.print()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            
            task1 = progress.add_task("🔌 Connessione al database...", total=None)
            
            try:
                # Connessione sicura
                from src.database.secure_connection import SecureDatabaseConnection
                self.db_connection = SecureDatabaseConnection(self.credentials, pool_size=2)
                
                progress.update(task1, description="✅ Connesso al database")
                
                # Inizializzazione schema
                task2 = progress.add_task("🏗️ Creazione tabelle enterprise...", total=None)
                
                self.schema_manager = SchemaManager(self.db_connection)
                
                if self.schema_manager.initialize_enterprise_schema():
                    progress.update(task2, description="✅ Schema enterprise inizializzato")
                    
                    # Verifica integrità
                    task3 = progress.add_task("🔍 Verifica integrità schema...", total=None)
                    
                    if self.schema_manager.verify_schema_integrity():
                        progress.update(task3, description="✅ Integrità schema verificata")
                    else:
                        progress.update(task3, description="⚠️ Problemi integrità schema")
                        self.console.print("⚠️ Alcuni problemi di integrità rilevati", style="yellow")
                
                else:
                    progress.update(task2, description="❌ Errore creazione schema")
                    return False
                
            except Exception as e:
                progress.update(task1, description="❌ Errore inizializzazione")
                self.console.print(f"\n🚨 Errore inizializzazione schema: {e}", style="red")
                return False
        
        # Mostra info schema
        schema_info = self.schema_manager.get_schema_info()
        self._show_schema_info(schema_info)
        
        return True
    
    def _show_schema_info(self, schema_info: Dict[str, Any]):
        """Mostra informazioni sullo schema creato."""
        table = Table(title="📊 Informazioni Schema Enterprise", box=box.ROUNDED)
        table.add_column("Proprietà", style="cyan")
        table.add_column("Valore", style="white")
        
        table.add_row("Versione Schema", schema_info.get('current_version', 'N/A'))
        table.add_row("Numero Tabelle", str(schema_info.get('table_count', 0)))
        table.add_row("Ultimo Aggiornamento", str(schema_info.get('last_update', 'N/A')))
        table.add_row("Schema Manager", schema_info.get('schema_manager_version', 'N/A'))
        
        self.console.print()
        self.console.print(table)
        self.console.print()
    
    def _save_credentials(self) -> bool:
        """Salva le credenziali in modo sicuro."""
        self.console.print("💾 [bold blue]STEP 5: Salvataggio Credenziali Sicure[/bold blue]")
        self.console.print()
        
        save_method = self.console.input(
            "Come vuoi salvare le credenziali?\n"
            "[1] Keyring di sistema (raccomandato)\n"
            "[2] Variabili d'ambiente\n"
            "[3] File .env (meno sicuro)\n"
            "Scegli [1-3]: "
        )
        
        if save_method == "1":
            return self._save_to_keyring()
        elif save_method == "2":
            return self._save_to_env_vars()
        elif save_method == "3":
            return self._save_to_env_file()
        else:
            self.console.print("❌ Opzione non valida", style="red")
            return self._save_credentials()
    
    def _save_to_keyring(self) -> bool:
        """Salva le credenziali nel keyring di sistema."""
        try:
            from src.security import SecureCredentialManager
            
            credential_manager = SecureCredentialManager()
            credential_manager._save_credentials_to_keyring(self.credentials)
            
            self.console.print("✅ [green]Credenziali salvate nel keyring di sistema[/green]")
            return True
            
        except Exception as e:
            self.console.print(f"❌ Errore salvataggio keyring: {e}", style="red")
            return False
    
    def _save_to_env_vars(self) -> bool:
        """Mostra le variabili d'ambiente da impostare."""
        env_commands = f"""
export MYSQL_HOST="{self.credentials['host']}"
export MYSQL_PORT="{self.credentials['port']}"
export MYSQL_USER="{self.credentials['user']}"
export MYSQL_PASSWORD="{self.credentials['password']}"
export MYSQL_DATABASE="{self.credentials['database']}"
export MYSQL_SSL_DISABLED="{str(self.credentials['ssl_disabled']).lower()}"
        """.strip()
        
        panel = Panel(
            env_commands,
            title="🔧 Variabili d'Ambiente da Impostare",
            title_align="left",
            border_style="green"
        )
        
        self.console.print()
        self.console.print(panel)
        self.console.print()
        self.console.print("💡 [yellow]Copia e incolla questi comandi nel tuo terminale[/yellow]")
        
        return True
    
    def _save_to_env_file(self) -> bool:
        """Salva le credenziali in un file .env."""
        env_content = f"""# ANAC Importer Enterprise - Credenziali Database
MYSQL_HOST={self.credentials['host']}
MYSQL_PORT={self.credentials['port']}
MYSQL_USER={self.credentials['user']}
MYSQL_PASSWORD={self.credentials['password']}
MYSQL_DATABASE={self.credentials['database']}
MYSQL_SSL_DISABLED={str(self.credentials['ssl_disabled']).lower()}
"""
        
        try:
            with open('.env', 'w') as f:
                f.write(env_content)
            
            self.console.print("✅ [green]Credenziali salvate in .env[/green]")
            self.console.print("⚠️ [yellow]ATTENZIONE: Non committare il file .env in git![/yellow]")
            
            # Aggiungi a .gitignore se esiste
            gitignore_path = Path('.gitignore')
            if gitignore_path.exists():
                with open(gitignore_path, 'a') as f:
                    f.write('\n.env\n')
                self.console.print("✅ [green].env aggiunto a .gitignore[/green]")
            
            return True
            
        except Exception as e:
            self.console.print(f"❌ Errore creazione .env: {e}", style="red")
            return False
    
    def _final_validation(self) -> bool:
        """Validazione finale del setup."""
        self.console.print("\n🔍 [bold blue]STEP 6: Validazione Finale[/bold blue]")
        self.console.print()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            
            # Test connessione completa
            task1 = progress.add_task("🔗 Test connessione completa...", total=None)
            
            try:
                if self.db_connection.test_connection():
                    progress.update(task1, description="✅ Connessione completa OK")
                else:
                    progress.update(task1, description="❌ Test connessione fallito")
                    return False
                
                # Test inserimento dati
                task2 = progress.add_task("📝 Test inserimento dati...", total=None)
                
                test_query = "INSERT INTO system_config (config_key, config_value, description) VALUES (%s, %s, %s)"
                test_data = ('wizard_setup_test', 'completed', 'Test inserimento dal wizard')
                
                result = self.db_connection.execute_with_retry(test_query, test_data)
                if result is not None:
                    progress.update(task2, description="✅ Test inserimento OK")
                else:
                    progress.update(task2, description="❌ Test inserimento fallito")
                    return False
                
                # Test lettura dati
                task3 = progress.add_task("📖 Test lettura dati...", total=None)
                
                read_query = "SELECT config_value FROM system_config WHERE config_key = %s"
                read_result = self.db_connection.execute_with_retry(read_query, ('wizard_setup_test',))
                
                if read_result and len(read_result) > 0:
                    progress.update(task3, description="✅ Test lettura OK")
                else:
                    progress.update(task3, description="❌ Test lettura fallito")
                    return False
                
            except Exception as e:
                self.console.print(f"\n🚨 Errore validazione finale: {e}", style="red")
                return False
        
        return True
    
    def _show_success(self):
        """Mostra il messaggio di successo finale."""
        success_text = """
🎉 SETUP COMPLETATO CON SUCCESSO!

Il tuo sistema ANAC Importer Enterprise è ora completamente configurato:

✅ Database configurato e pronto
✅ Schema enterprise inizializzato
✅ Credenziali salvate in modo sicuro
✅ Connettività verificata
✅ Sistema pronto per l'uso

PROSSIMI PASSI:

1. Testare l'importazione:
   python3 src/main_enterprise.py import-files file.json

2. Verificare configurazione:
   python3 src/main_enterprise.py test-connection

3. Consultare la documentazione:
   cat IMPLEMENTAZIONE_COMPLETA_ENTERPRISE.md

Buon lavoro con ANAC Importer Enterprise! 🚀
        """
        
        panel = Panel(
            success_text.strip(),
            title="🎊 Setup Completato",
            title_align="left",
            border_style="green",
            box=box.DOUBLE
        )
        
        self.console.print()
        self.console.print(panel)
        self.console.print()

# Funzioni CLI
@click.command()
def setup_wizard():
    """🧙‍♂️ Avvia il wizard di setup interattivo."""
    wizard = SetupWizard()
    
    try:
        success = wizard.run_wizard()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        console.print("\n❌ Setup interrotto dall'utente", style="red")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n🚨 Errore critico: {e}", style="red")
        sys.exit(1)

if __name__ == "__main__":
    setup_wizard()