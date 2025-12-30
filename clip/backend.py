import os
import sys
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

# 添加项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(PROJECT_ROOT)
sys.path.insert(0, PROJECT_ROOT)

# 导入你的CLIP嵌入和向量库操作类
from clip.embedding import CLIPEmbedding
from clip.db_operation import VectorDBOperation

# 初始化FastAPI
app = FastAPI()

# 解决跨域问题
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境替换为前端域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载静态文件：让前端能访问图片（替换为你的图片根目录）
IMAGE_ROOT = os.path.join(PROJECT_ROOT, "Data")  # 你的图片存储目录
app.mount("/static", StaticFiles(directory=IMAGE_ROOT), name="static")

# 初始化向量库操作类（全局单例）
db_op = VectorDBOperation()

# 检索接口
@app.post("/api/search")
async def search(request: Request):
    try:
        data = await request.json()
        text = data.get("text", "")
        topk = int(data.get("topk", 5))
        
        # 调用向量库检索（现在返回带分数的列表）
        search_result = db_op.text_search(text, topk)
        
        # 构造返回数据
        return JSONResponse({
            "success": True,
            "results": search_result  # 替换原来的imagePaths，返回带分数的列表
        })
    except Exception as e:
        return JSONResponse({
            "success": False,
            "results": [],
            "error": str(e)
        }, status_code=500)

if __name__ == "__main__":
    import uvicorn
    # 启动后端服务：http://127.0.0.1:8000
    uvicorn.run(app, host="127.0.0.1", port=8000)