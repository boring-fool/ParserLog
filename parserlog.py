import datetime
import re
import threading
from pathlib import Path
from queue import Queue
from user_agents import parse
from collections import defaultdict

log = '''123.125.71.36 - - [06/Apr/2017:18:09:25 +0800] "GET / HTTP/1.1" 200 8642 "-" "Mozilla/5.0 
(compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)"'''
patrren = '''(?P<remote>[\d\.]{7,}) - - \[(?P<datetime>[^\[\]]+)\] "(?P<request>[^"]+)" (?P<status>[\d]+) (?P<size>[\d]+) "[^"]+" "(?P<useragent>[^"]+)"'''
regex = re.compile(patrren)
def extract(line):
    matcher = regex.match(line)
    return {k:ops.get(k,lambda x:x)(v) for k,v in matcher.groupdict().items()}
ops ={
    'datetime' : lambda timestr:datetime.datetime.strptime(timestr,"%d/%b/%Y:%H:%M:%S %z"),
    'status' : int,
    'size' : int,
    'request' : lambda request:dict(zip(['method','url','protool'],request.split()))
}
def open_file(path):
    with open(path) as file:
        for line in file:
            field = extract(line)
            if field:
                yield field
            else:
                # TODO mismatching data.
                continue
def load(*path:Path):
    for item in path:
        pth = Path(item)
        if not pth.exists():
            continue
        if pth.is_dir():
            for file in pth.iterdir():
                if file.is_file():
                    yield from open_file(file)
        elif pth.is_file():
            yield from open_file(pth)

def donothing_handler(iterable):
    print(iterable)
def status_handler(iterable):
    status = {}
    for data in iterable:
        status[data['status']] = status.get(data['status'],0)+1
    total = sum(status.values())
    return {k:v/total*100 for k,v in status.items() }
ua_dict = defaultdict(lambda :0)
def browser_handler(iterable):
    for ua in iterable:
        ua_parser = parse(ua['useragent'])
        key = (ua_parser.browser.family,ua_parser.browser.version_string)
        ua_dict[key] += 1
    return ua_dict
def window(src:Queue,handler,width:int,interval:int):

    start = datetime.datetime.strptime('20170101 00000 +0800','%Y%m%d %H%M%S %z')
    current = datetime.datetime.strptime('20170101 00000 +0800', '%Y%m%d %H%M%S %z')
    delta = datetime.timedelta(width - interval)
    buffers = []
    while True:
        data = src.get()
        if data:
            buffers.append(data)
            current = data['datetime']
        if (current - start).total_seconds() >=interval:
            ret = handler(buffers)
            print(ret)
            start = current
            buffers = [data for data in buffers if data['datetime'] > (current - delta)]


def dispatch(src):
    queues = []
    threads = []
    def reg(handler,width,interval):
        q = Queue()
        queues.append(q)

        h = threading.Thread(target = window,args=(q,handler,width,interval))
        threads.append(h)
    def run():
        for t in threads:
            t.start()
        for item in src:
            for q in queues:
                q.put(item)
    return reg,run

ret,runs = dispatch(load('test.log'))
ret(status_handler,10,5)
ret(browser_handler,10,5)
runs()
