file_path = "./Data/xiyouji.txt"
with open(file_path, 'r', encoding='gbk',errors='ignore') as f:
    content = f.read()
with open("./Data/xiyouji_utf8.txt", 'w', encoding='utf-8') as f:
    f.write(content)