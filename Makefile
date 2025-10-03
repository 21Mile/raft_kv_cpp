
# 定义构建目录和二进制输出目录 [3,7](@ref)
BUILD_DIR := build
BIN_DIR := bin
.PHONY: all clean

all:
	@mkdir -p $(BUILD_DIR) $(bin)
	cmake -B $(BUILD_DIR)  -DCMAKE_BUILD_TYPE=Release
	cmake --build $(BUILD_DIR)  --parallel

clean:
	@echo "正在清理构建文件和二进制..."
	@rm -rf build/*
	@rm -rf bin/*
	@echo "清理完成"

rebuild: clean all

# 帮助信息 [1,6](@ref)
.PHONY: help
help:
	@echo "可用目标:"
	@echo "  all       : 配置并编译项目 (默认)"
	@echo "  configure : 仅生成构建系统"
	@echo "  build     : 仅编译项目"
	@echo "  install   : 安装到系统路径"
	@echo "  clean     : 删除所有构建产物"
	@echo "  rebuild   : 完全重建项目"
	@echo "  help      : 显示此帮助"