import sys
from loguru import logger

def initLogger(logFilePath):
    logger.remove(0)
    logger.add(sys.stderr, format="{time:HH:mm:ss.SS} | {level} | {message}")    
    logger.add(logFilePath)
    return logger