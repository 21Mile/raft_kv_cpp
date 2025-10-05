# --- 可调参数 -----------------------------------------------------
BUILD_DIR ?= out/build
BIN_DIR   ?= bin
BUILD_TYPE ?= Debug               # Debug / Release / RelWithDebInfo / MinSizeRel
JOBS ?=                            # 留空=由 CMake 自己决定；或设置 JOBS=8

# 自动选择生成器：优先 Ninja
CMAKE_GENERATOR ?= $(shell command -v ninja >/dev/null 2>&1 && echo Ninja || echo "Unix Makefiles")

# 额外 CMake 选项（可在命令行传入 EXTRA=...）
EXTRA ?=
# -----------------------------------------------------------------

.PHONY: all configure build install clean rebuild help

all: build

configure:
	@echo "==> Configure with CMake (generator: $(CMAKE_GENERATOR), type: $(BUILD_TYPE))"
	@mkdir -p "$(BUILD_DIR)" "$(BIN_DIR)"
	@cmake -S . -B "$(BUILD_DIR)" \
		-G "$(CMAKE_GENERATOR)" \
		-DCMAKE_BUILD_TYPE="$(BUILD_TYPE)" \
		-DCMAKE_RUNTIME_OUTPUT_DIRECTORY="$(abspath $(BIN_DIR))" \
		-DCMAKE_LIBRARY_OUTPUT_DIRECTORY="$(abspath $(BIN_DIR))" \
		-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY="$(abspath $(BIN_DIR))" \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
		$(EXTRA)

build: configure
	@echo "==> Build"
	@cmake --build "$(BUILD_DIR)" $(if $(JOBS),--parallel $(JOBS),--parallel)

install: build
	@echo "==> Install"
	@cmake --install "$(BUILD_DIR)"

clean:
	@echo "==> 清理构建产物与输出目录"
	@rm -rf "$(BUILD_DIR)" "$(BIN_DIR)" out data logs lib
	@echo "完成"

rebuild: clean all

help:
	@echo "可用目标："
	@echo "  make [all]           : 配置并编译项目（默认）"
	@echo "  make configure       : 仅生成构建系统"
	@echo "  make build           : 仅编译（会先确保已 configure）"
	@echo "  make install         : 安装到系统路径（取决于 CMake install()）"
	@echo "  make clean           : 清理构建产物"
	@echo "  make rebuild         : 重新全量构建"
	@echo ""
	@echo "常用可选参数："
	@echo "  BUILD_TYPE=Release   # 或 Debug/RelWithDebInfo/MinSizeRel"
	@echo "  JOBS=8               # 指定并行编译线程数"
	@echo "  EXTRA='-DFOO=ON'     # 传递额外的 CMake 选项"
