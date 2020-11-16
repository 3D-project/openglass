import json
import os
import platform

try:
    import pwd
except:
    pass

class Settings(object):
    """
    This class stores all of the settings for openglass.
    """

    def __init__(self, utility, config=False):
        self.utility = utility

        self.utility.log("Settings", "__init__")

        # If a readable config file was provided, use that instead
        if config:
            if os.path.isfile(config):
                self.filename = config
            else:
                self.utility.log(
                    "Settings",
                    "__init__",
                    "Supplied config does not exist or is unreadable. Falling back to default location",
                )
                self.filename = self.build_filename()

        else:
            # Default config
            self.filename = self.build_filename()

        # These are the default settings. They will get overwritten when loading from disk
        self.default_settings = {
            "version": self.utility.version,
            "twitter_apis": [],
        }
        self._settings = {}
        self.fill_in_defaults()

    def fill_in_defaults(self):
        """
        If there are any missing settings from self._settings, replace them with
        their default values.
        """
        for key in self.default_settings:
            if key not in self._settings:
                self._settings[key] = self.default_settings[key]

    def build_filename(self):
        """
        Returns the path of the settings file.
        """
        return os.path.join(self.utility.build_data_dir(), "config.json")

    def print_settings(self):
        return json.dumps(self._settings, indent=4, sort_keys=True)

    def load(self):
        """
        Load the settings from file.
        """
        self.utility.log("Settings", "load")

        # If the settings file exists, load it
        if os.path.exists(self.filename):
            try:
                self.utility.log("Settings", "load", f"Trying to load {self.filename}")
                with open(self.filename, "r") as f:
                    self._settings = json.loads(f.read())
                    self.fill_in_defaults()
            except:
                pass

    def save(self):
        """
        Save settings to file.
        """
        self.utility.log("Settings", "save")
        open(self.filename, "w").write(json.dumps(self._settings, indent=2))
        self.utility.log("Settings", "save", f"Settings saved in {self.filename}")

    def get(self, key):
        return self._settings[key]

    def set(self, key, val):
        self._settings[key] = val
