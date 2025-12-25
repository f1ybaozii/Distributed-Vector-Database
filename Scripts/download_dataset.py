import os
import zipfile
from sentence_transformers import util

# 创建数据集目录
os.makedirs('./test_data/unsplash', exist_ok=True)
# 下载压缩包
util.http_get('http://sbert.net/datasets/unsplash-25k-photos.zip', './test_data/unsplash.zip')
# 解压
with zipfile.ZipFile('./test_data/unsplash.zip', 'r') as zip_ref:
    zip_ref.extractall('./test_data/unsplash/')
print('数据集下载完成，路径：./test_data/unsplash')