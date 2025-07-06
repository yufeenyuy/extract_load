import logging as lg
from datetime import datetime

lg.basicConfig(filename=f"dataingest_{datetime.now().strftime('%Y-%m-%d')}.log", encoding='utf-8', format='%(levelname)s: %(asctime)s %(message)s.', datefmt='%d.%m.%Y %I:%M:%S', level=lg.DEBUG) 