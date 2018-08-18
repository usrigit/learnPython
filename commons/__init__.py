import os
import configparser

config = configparser.RawConfigParser(interpolation=configparser.ExtendedInterpolation())
config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))

def get_prop(section, key):
    config_val = config.get(section, key)
    return config_val
