import json
import os
import platform
import sys

from .settings import Settings


class Utility:
    """
    The Utility object is shared amongst all parts of openglass.
    """

    def __init__(self, verbose=False):
        self.verbose = verbose

        # The platform Openglass is running on
        self.platform = platform.system()
        if self.platform.endswith("BSD") or self.platform == "DragonFly":
            self.platform = "BSD"

        path = os.path.dirname(os.path.realpath(__file__))
        path = os.path.join(path, "../share/version.txt")
        with open(path) as f:
            self.version = f.read().strip()

    def load_settings(self, config=None):
        """
        Loading settings, optionally from a custom config json file.
        """
        self.settings = Settings(self, config)
        self.settings.load()

    def print_settings(self, config=None):
        self.settings = Settings(self, config)
        self.settings.load()
        return self.settings.print_settings()

    def get_setting(self, value):
        return self.settings.get(value)

    def build_data_dir(self):
        """
        Returns the path of the openglass data directory.
        """
        if self.platform == "Windows":
            try:
                appdata = os.environ["APPDATA"]
                openglass_data_dir = f"{appdata}\\openglass"
            except Exception as e:
                # If for some reason we don't have the 'APPDATA' environment variable
                # (like running tests in Linux while pretending to be in Windows)
                openglass_data_dir = os.path.expanduser("~/.config/openglass")
        elif self.platform == "Darwin":
            openglass_data_dir = os.path.expanduser(
                "~/Library/Application Support/openglass"
            )
        else:
            openglass_data_dir = os.path.expanduser("~/.config/openglass")

        # Modify the data dir if running tests
        if getattr(sys, "openglass_test_mode", False):
            openglass_data_dir += "-testdata"

        os.makedirs(openglass_data_dir, 0o700, True)
        return openglass_data_dir

    def build_tmp_dir(self):
        """
        Returns path to a folder that can hold temporary files
        """
        tmp_dir = os.path.join(self.build_data_dir(), "tmp")
        os.makedirs(tmp_dir, 0o700, True)
        return tmp_dir

    def build_persistent_dir(self):
        """
        Returns the path to the folder that holds persistent files
        """
        persistent_dir = os.path.join(self.build_data_dir(), "persistent")
        os.makedirs(persistent_dir, 0o700, True)
        return persistent_dir

    def log(self, module, func, msg=None):
        """
        If verbose mode is on, log error messages to stdout
        """
        if self.verbose:
            timestamp = time.strftime("%b %d %Y %X")

            final_msg = f"[{timestamp}] {module}.{func}"
            if msg:
                final_msg = f"{final_msg}: {msg}"
            print(final_msg)
