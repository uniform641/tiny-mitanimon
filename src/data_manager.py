import configparser

class DataManager:
    config : configparser.ConfigParser
    def __init__(self, config_file_path : str):
        self.config = configparser.ConfigParser()
        self.config.read(config_file_path)
    
    def download(self):
        pass
