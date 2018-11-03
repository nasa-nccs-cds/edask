import logging, os, time, socket

class EDASLogger:
    logger = None

    @classmethod
    def getLogger(cls):
        if cls.logger is None:
            LOG_DIR = os.path.expanduser("~/.edask/logs")
            if not os.path.exists(LOG_DIR):  os.makedirs(LOG_DIR)
            timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", time.gmtime())
            cls.logger = logging.getLogger()
            cls.logger.setLevel(logging.DEBUG)
            fh = logging.FileHandler("{}/edask-{}-{}.log".format(LOG_DIR, socket.gethostname(), timestamp))
            fh.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            cls.logger.addHandler(fh)
            cls.logger.addHandler(ch)
        return cls.logger
