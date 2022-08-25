from pyarrow.parquet import ParquetFile
import pyarrow as pa
import json

pf = ParquetFile('test.parquet')
partNum = 0


def writeToFile(data, part):
    fileName = 'import-part-' + str(part).zfill(6) + ".json"
    with open(fileName, 'w') as f:
        f.write(data)
        f.close()


if __name__ == '__main__':
    while True:
        try:
            first_ten_rows = next(pf.iter_batches(batch_size=1000))
            df = pa.Table.from_batches([first_ten_rows]).to_pandas()
            jsonstr = df.to_json(orient='records')
            writeToFile(jsonstr, partNum)
            partNum += 1
        except StopIteration:
            break