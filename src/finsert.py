import argparse
import os, json, glob

import pandas as pd
from pandas._config import config
from dask import dataframe as dd
import dask
import dask.bag as db
from elasticsearch import Elasticsearch, helpers

from utils import Config

class ElasticEngine(object):

    def __init__(self, index) -> None:
        self.index = index
        self.engine = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.engine.indices.create(index=index, ignore=400)

    def read_file(self, file):
        if file.endswith('csv'):
            return dd.read_csv(file, dtype='str')
        if file.split('.')[-1] in ['xlsx', 'xls']:
            # TODO: Need to change for multiple sheets
            parts = dask.delayed(pd.read_excel)(file, sheet_name=0, dtype='str')
            return dd.from_delayed(parts)
        if file.endswith('json'):
            jsonbag = db.read_text(file).map(json.loads)
            return jsonbag.to_dataframe()
        # TODO: Add support for sql file
        # if file.endswith('sql'):
        #     return pd.read_sql(file)

    def generator(self, df):
        # find index
        index = ''
        for col in df.columns:
            if df[col].isna().sum().compute() == 0:
                index = col
                break
        
        for i, row in df.iterrows():
            # print(i, row)
            # Find datatypes
            data = {}
            for col in df.columns:
                try:
                    data[col] = int(row[col])
                except:
                    try:
                        data[col] = float(row[col])
                    except:
                        data[col] = row[col]
            yield {
                "_index": self.index,
                "_type": "_doc",
                "_id": str(row.get(index, str(i))),
                "_source": data
            }
        raise StopIteration

    def process(self, files) -> None:
        # process all files in bulk
        for file in files:
            if file.split('.')[-1] not in Config.EXTENSIONS:
                continue
            print("[INFO] file->", file)
            df = self.read_file(file)
            # df = df[df.columns].astype(str)
            try:
                # print(next(self.generator(df)))
                helpers.bulk(self.engine, self.generator(df))
            except Exception as e:
                print(f"[ERROR] in {file} ", e) 

class Finsert(object):

    def __init__(self, config) -> None:
        self.config = config
        self.engine = ElasticEngine(config.INDEX)

    def get_files(self) -> list:
        if not self.config.RECURSIVE:
            files = glob.glob(self.config.FILES_DIR + '*.' + self.config.EXTENSION)
            return files
        
        queue = [self.config.FILES_DIR]
        files = []
        while len(queue) > 0:
            dir = queue.pop(0) + '/*'
            files += glob.glob(dir + f"*.{self.config.EXTENSION}")
            queue += [dir for dir in glob.glob(dir + '*') if os.path.isdir(dir)]
        return files

    def start(self) -> None:
        files = self.get_files()
        # print(files)
        print("[INFO] Files found", len(files))
        # distribute the files
        self.engine.process(files[:1])

def getParser():
    parser = argparse.ArgumentParser(description='Finsert help commands:')
    parser.add_argument('-f', '--files', help='Files root directory', required=True)
    parser.add_argument('-r', '--recursive', help='Find files recursively', default=False, action='store_true')
    parser.add_argument('-e', '--extension', help='Specify a specific extension like csv', default='*')
    parser.add_argument('-i', '--index', help='Index name for elastic search', required=True)
    parser.add_argument('-p', '--processes', help="Number of distributed processes to insert data", default=8)
    args = parser.parse_args()
    return args

def main() -> None:
    args = getParser()
    config = Config(args)
    print( "[INFO] Config: \n", config)
    fi = Finsert(config)
    fi.start()
    # mexican_citizen

if __name__ == "__main__":
    main()