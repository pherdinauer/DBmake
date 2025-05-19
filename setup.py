from pathlib import Path
import shutil
import sys
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_directories():
    """Create the necessary directory structure for the application."""
    # Get the workspace path
    workspace_path = Path(__file__).parent
    
    # Define directories to create
    directories = [
        workspace_path / "data" / "downloads",
        workspace_path / "database",
        workspace_path / "database" / "backups",
        workspace_path / "logs"
    ]
    
    # Create directories
    for directory in directories:
        try:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"✅ Directory created: {directory}")
        except Exception as e:
            logger.error(f"❌ Error creating directory {directory}: {e}")
            sys.exit(1)
    
    # Create subdirectories for each category
    from config.config import CARTELLE_RILEVANTI
    for category in CARTELLE_RILEVANTI:
        category_path = workspace_path / "data" / "downloads" / category
        try:
            category_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"✅ Category directory created: {category_path}")
        except Exception as e:
            logger.error(f"❌ Error creating category directory {category_path}: {e}")
            sys.exit(1)

if __name__ == "__main__":
    logger.info("🚀 Setting up directory structure...")
    setup_directories()
    logger.info("✅ Setup completed successfully") 