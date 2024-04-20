from pathlib import Path

import yaml


class Config:
    """
    Configuration class for astra. It loads the config file and creates the
    assets folder if they do not exist.
    """

    folder_config: str = Path.home() / ".astra"
    """folder where astra config files are located"""

    file_config: str = folder_config / "config.yaml"
    """config file path"""

    folder_assets: None
    """folder where working sub-folders like schedule, log... are located"""

    def __init__(self):
        self.folder_config.mkdir(exist_ok=True)
        self.config = self.load_config()
        self.check_assets_folders("log", "schedule", "observatory", "temp", "images")

    @property
    def folder_observatory(self):
        """Folder where telescope configuration are stored."""
        return self.folder_assets / "observatory"

    @property
    def folder_schedule(self):
        """Folder where schedule files are stored."""
        return self.folder_assets / "schedule"

    @property
    def folder_log(self):
        """Folder where log files are stored."""
        return self.folder_assets / "log"

    @property
    def folder_temp(self):
        """Folder where temporary files are stored."""
        return self.folder_assets / "temp"

    @property
    def folder_images(self):
        """Folder where image files are stored."""
        return self.folder_assets / "images"

    @property
    def file_log(self):
        """Log file path."""
        return self.folder_log / "astra.log"

    def check_assets_folders(self, *names, exist_ok=True):
        """Check if the assets folders exist, if not create them, i.e.
        telescope, schedule, log. Base folder is defined in the config file as
        ``folder_assets``.

        Parameters
        ----------
        exist_ok : bool, optional
            If True, do not raise an exception if the folder already exists.
            Default is True.
        """
        self.folder_assets = Path(self.config["folder_assets"])
        self.folder_assets.mkdir(exist_ok=exist_ok)

        for folder in names:
            if not (self.folder_assets / folder).exists():
                (self.folder_assets / folder).mkdir()
                print(f"Created folder {self.folder_assets / folder}")

        # write onto log file (create if not exist)
        with open(self.file_log, "a") as file:
            file.write("")

    def check_config_file(self, exist_ok=True):
        """Check if the config file exists, if not create it.

        Parameters
        ----------
        exist_ok : bool, optional
            If True, do not raise an exception if the file already exists.
            Default is True.

        Raises
        ------
        FileExistsError
            If the file already exists and `exist_ok` is False.
        """
        if not self.file_config.exists():
            if not exist_ok:
                raise FileExistsError("Config file already exists")
            else:
                config = {
                    "folder_assets": str(
                        Path(__file__).parent.parent.parent / "assets"
                    ),
                    "gaia_db": None,
                }

                with open(self.file_config, "w") as file:
                    yaml.dump(config, file)
                    print(f"Created config file {self.file_config}")

    def load_config(self):
        """Load the config file."""
        self.check_config_file()

        with open(self.file_config, "r") as file:
            return yaml.safe_load(file)
