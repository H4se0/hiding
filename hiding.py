#!/opt/homebrew/bin/python3.11
import os
import sqlite3
import subprocess
import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Konfiguracja
HOME_DIR = '/Users/user_name'
EXCLUDED_DIRS = {'Music', 'Pictures', 'Library'} 
DB_FILE = '/Users/user_name/your_dir/hidden.db'
LOG_FILE = '/Users/user_name/your_dir/hidden.log'

def log_message(message, print_to_console=True):
    """Loguje wiadomość do pliku logu i opcjonalnie wypisuje na konsolę."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}\n"
    
    with open(LOG_FILE, "a") as log:
        log.write(log_entry)
    
    if print_to_console:
        print(log_entry.strip())

# Połączenie z bazą danych
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Tworzenie tabeli jeśli nie istnieje
cursor.execute('''
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        path TEXT,
        extension TEXT,
        is_processed INTEGER,
        date TEXT,
        size INTEGER
    )''')
conn.commit()

# Dodaj indeks (jeśli już istnieje, SQLite to zignoruje)
cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_name_path ON files (file_name, path);')
conn.commit()

def insert_file_entries(entries):
    """Masowe wstawianie wpisów do bazy."""
    cursor.executemany('''
        INSERT INTO files (file_name, path, extension, is_processed, date, size)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', entries)
    conn.commit()

def file_entry_exists(file_name, path):
    """Sprawdza czy plik istnieje w bazie."""
    cursor.execute('SELECT 1 FROM files WHERE file_name = ? AND path = ? LIMIT 1', (file_name, path))
    return cursor.fetchone() is not None

def prune_missing_files():
    """Usuwa z bazy wpisy plików, które nie istnieją na dysku."""
    cursor.execute("SELECT id, file_name, path FROM files")
    to_delete = [(row[0],) for row in cursor.fetchall() if not os.path.exists(os.path.join(row[2], row[1]))]
    
    if to_delete:
        cursor.executemany("DELETE FROM files WHERE id = ?", to_delete)
        conn.commit()
    
    log_message(f"Pruned {len(to_delete)} missing files.")

def hide_file_extension(file_path):
    subprocess.run(['SetFile', '-a', 'E', file_path])

def get_hidden_extension(file_path):
    """Sprawdza, czy rozszerzenie pliku jest ukryte przy użyciu GetFileInfo."""
    try:
        result = subprocess.run(
            ["GetFileInfo", "-ae", file_path],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip() == "1"  # Jeśli "1", oznacza ukryte rozszerzenie
    except subprocess.CalledProcessError:
        return False

def process_file(file_path, file_name, root):
    """Przetwarza pojedynczy plik, sprawdzając ukryte rozszerzenie i dodając go do listy nowych wpisów."""
    try:
        hidden_extension = get_hidden_extension(file_path)
        extension = os.path.splitext(file_name)[1]
        is_processed = 1 if hidden_extension else 0

        if not hidden_extension:
            hide_file_extension(file_path)

        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        size = os.path.getsize(file_path)

        return (file_name, root, extension, is_processed, date, size)

    except Exception as e:
        log_message(f'ERROR: {str(e)} on file: {file_path}')
        return None

def process_files():
    """Przetwarza pliki w katalogu domowym."""
    log_message("Processing files started.")
    
    new_entries = []
    total_files = 0
    skipped_files = 0
    files_to_process = []

    for root, dirs, files in os.walk(HOME_DIR):
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in EXCLUDED_DIRS]
        for file_name in files:
            if not file_name.startswith('.') and not os.path.islink(os.path.join(root, file_name)):
                files_to_process.append((root, file_name))

    with tqdm(total=len(files_to_process), desc="Processing files", unit="file") as pbar:
        with ThreadPoolExecutor() as executor:
            futures = []
            for root, file_name in files_to_process:
                file_path = os.path.join(root, file_name)
                if file_entry_exists(file_name, root):
                    skipped_files += 1
                    pbar.update(1)
                    continue

                futures.append(executor.submit(process_file, file_path, file_name, root))

            for future in futures:
                result = future.result()
                if result:
                    new_entries.append(result)

                total_files += 1
                pbar.update(1)

    if new_entries:
        insert_file_entries(new_entries)

    log_message(f'New entries: {len(new_entries)}')
    log_message(f'Skipped files: {skipped_files}')
    log_message(f'Total files processed: {total_files}')
    log_message("Processing files finished.")

# Wykonanie przetwarzania
log_message("Script started.")
process_files()
prune_missing_files()

# Zamknięcie bazy
total_entries = cursor.execute("SELECT COUNT(*) FROM files").fetchone()[0]
log_message(f'Total records in DB: {total_entries}')
log_message("Script finished.")

conn.close()