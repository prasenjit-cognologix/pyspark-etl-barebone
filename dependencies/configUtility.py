import json

def loadConfig():
    fileName = 'configs/test_config.json'
    with open(fileName) as config_file:
        data = json.load(config_file)
        return data