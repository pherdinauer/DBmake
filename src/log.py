import logging
import os
from datetime import datetime

# Crea la directory logs se non esiste
os.makedirs('logs', exist_ok=True)

# Configura il logger
logger = logging.getLogger('anac_importer')
logger.setLevel(logging.INFO)

# Formato del log
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Handler per file
log_file = os.path.join('logs', f'anac_importer_{datetime.now().strftime("%Y%m%d")}.log')
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Handler per console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler) 