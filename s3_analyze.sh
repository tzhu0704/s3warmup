#!/bin/bash

# S3目录结构分析工具
# 用于分析S3 bucket的目录结构，并提供Lustre条带化建议
# 用法: ./s3_analyze.sh [-b] <bucket> <prefix> <depth> [sample_size]

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 检查是否后台运行模式
BACKGROUND=false
while getopts ":b" opt; do
  case ${opt} in
    b )
      BACKGROUND=true
      ;;
    \? )
      echo -e "${RED}错误: 无效选项 -$OPTARG${NC}" 1>&2
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

# 检查参数
if [ $# -lt 3 ]; then
    echo -e "${RED}错误: 参数不足${NC}"
    echo -e "用法: $0 [-b] <bucket> <prefix> <depth> [sample_size]"
    echo -e "  -b: 后台运行模式"
    echo -e "  bucket: S3 bucket名称"
    echo -e "  prefix: 要分析的前缀路径，使用'/'表示根目录"
    echo -e "  depth: 递归分析的深度"
    echo -e "  sample_size: 每个目录采样的对象数量，默认1000"
    exit 1
fi

# 参数解析
BUCKET=$1
PREFIX=$2
DEPTH=$3
SAMPLE_SIZE=${4:-1000} # 默认采样量改为1000

# 修复 SAMPLE_SIZE 的赋值逻辑，确保其为整数
if ! [[ "$SAMPLE_SIZE" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}错误: sample_size 必须是整数${NC}"
    echo -e "用法: $0 [-b] <bucket> <prefix> <depth> [sample_size]"
    echo -e "  -b: 后台运行模式"
    echo -e "  bucket: S3 bucket名称"
    echo -e "  prefix: 要分析的前缀路径，使用'/'表示根目录"
    echo -e "  depth: 递归分析的深度"
    echo -e "  sample_size: 每个目录采样的对象数量，默认1000"
    exit 1
fi

# 修正前缀
# 如果前缀是单独的斜杠"/"，则将其转换为空字符串
if [ "$PREFIX" = "/" ]; then
    PREFIX=""
    if ! $BACKGROUND; then
        echo -e "${YELLOW}注意: '/' 前缀已转换为空前缀，将分析整个bucket${NC}"
    fi
fi

# 创建输出目录
OUTPUT_DIR="./s3_analysis_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"
OBJECTS_FILE="$OUTPUT_DIR/objects.txt"
PREFIXES_FILE="$OUTPUT_DIR/prefixes.txt"
STATS_FILE="$OUTPUT_DIR/stats.txt"
LOG_FILE="$OUTPUT_DIR/analysis.log"
TOP_DIRS_FILE="$OUTPUT_DIR/top_dirs.txt"
PREFIX_STATS_FILE="$OUTPUT_DIR/prefix_stats.txt"
VISITED_DIRS_FILE="$OUTPUT_DIR/visited_dirs.txt"

# 如果是后台模式，启动后台进程并退出
if $BACKGROUND; then
    echo -e "${BLUE}=== S3目录结构分析工具 ===${NC}"
    echo -e "分析在后台启动"
    echo -e "分析bucket: ${GREEN}$BUCKET${NC}"
    echo -e "分析prefix: ${GREEN}$PREFIX${NC}"
    echo -e "分析深度: ${GREEN}$DEPTH${NC}"
    echo -e "采样大小: ${GREEN}每个目录$SAMPLE_SIZE${NC}"
    echo -e "结果将保存到: ${GREEN}$OUTPUT_DIR${NC}"
    echo -e "您可以通过以下命令查看分析进度:"
    echo -e "  ${YELLOW}tail -f $LOG_FILE${NC}"
    echo -e "分析完成后，结果将保存在: ${GREEN}$STATS_FILE${NC}"
    
    # 启动相同的脚本，但不带 -b 参数，以在后台运行
    nohup "$0" "$BUCKET" "$PREFIX" "$DEPTH" "$SAMPLE_SIZE" > "$LOG_FILE" 2>&1 &
    NOHUP_PID=$!
    echo -e "${YELLOW}后台进程ID: $NOHUP_PID${NC}"
    exit 0
fi

# 下面是前台运行模式的代码
# 记录开始时间
START_TIME=$(date +%s)
echo -e "${BLUE}=== S3目录结构分析工具 ===${NC}"
echo -e "分析bucket: ${GREEN}$BUCKET${NC}"
echo -e "分析prefix: ${GREEN}$PREFIX${NC}"
echo -e "分析深度: ${GREEN}$DEPTH${NC}"
echo -e "采样大小: ${GREEN}每个目录$SAMPLE_SIZE${NC}"
echo -e "结果目录: ${GREEN}$OUTPUT_DIR${NC}"
echo -e "${BLUE}===========================${NC}"

# 记录日志
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

log_message "开始分析 bucket: $BUCKET, prefix: $PREFIX, depth: $DEPTH, sample_size: $SAMPLE_SIZE"

# 检查必要的命令是否可用
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${RED}错误: 未找到命令 '$1'。请安装后再运行。${NC}"
        log_message "错误: 未找到命令 '$1'"
        exit 1
    fi
}

check_command aws
check_command jq
check_command bc

# 格式化文件大小
format_size() {
    local size=$1
    if [ -z "$size" ] || [ "$size" -eq 0 ]; then
        echo "0B"
        return
    fi
    
    if (( size < 1024 )); then
        echo "${size}B"
    elif (( size < 1048576 )); then
        echo "$(echo "scale=2; $size/1024" | bc)KB"
    elif (( size < 1073741824 )); then
        echo "$(echo "scale=2; $size/1048576" | bc)MB"
    else
        echo "$(echo "scale=2; $size/1073741824" | bc)GB"
    fi
}

# 检查bucket是否存在
echo -n "检查Bucket存在性... "
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    echo -e "${RED}失败${NC}"
    echo -e "${RED}错误: Bucket '$BUCKET' 不存在或无权限访问${NC}"
    log_message "错误: Bucket '$BUCKET' 不存在或无权限访问"
    exit 1
fi
echo -e "${GREEN}成功${NC}"
log_message "Bucket存在性检查成功"

# 创建一个文件来跟踪已经访问过的前缀
> "$VISITED_DIRS_FILE"
> "$PREFIX_STATS_FILE"

# 清空对象文件
> "$OBJECTS_FILE"

# 获取对象的函数
get_objects() {
    local prefix="$1"
    local marker=""
    local continue=true
    local count=0
    local total_size=0
    local batch_size=1000  # 每次API调用的批次大小
    
    # 检查是否已经处理过此前缀
    if grep -q "^$prefix$" "$VISITED_DIRS_FILE"; then
        log_message "前缀 '$prefix' 已被处理过，跳过"
        return
    fi
    
    # 标记此前缀为已访问
    echo "$prefix" >> "$VISITED_DIRS_FILE"
    
    log_message "获取前缀 '$prefix' 的对象列表 (最多 $SAMPLE_SIZE 个对象)"
    
    # 如果前缀不是空的，显示正在处理的前缀
    local display_prefix="$prefix"
    [ -z "$display_prefix" ] && display_prefix="(根目录)"
    echo -n "获取 ${display_prefix} 对象列表..."
    
    # 循环获取对象，直到达到样本大小或没有更多对象
    while $continue && [ "$count" -lt "$SAMPLE_SIZE" ]; do
        # 调整批次大小，确保不超过总采样量
        local remaining=$((SAMPLE_SIZE - count))
        [ "$remaining" -lt "$batch_size" ] && batch_size=$remaining
        
        # 构建命令
        local cmd="aws s3api list-objects-v2 --bucket \"$BUCKET\" --prefix \"$prefix\" --max-items $batch_size"
        
        # 如果有续传标记，添加到命令中
        if [ -n "$marker" ]; then
            cmd="$cmd --starting-token \"$marker\""
        fi
        
        # 执行命令并获取结果
        local result=$(eval "$cmd")
        
        # 提取对象信息并添加到文件中
        local objects=$(echo "$result" | jq -r '.Contents[]? | [.Size, .Key] | @tsv' 2>/dev/null)
        
        if [ -n "$objects" ]; then
            # 过滤掉结尾是斜杠的条目（通常是目录标记）
            local filtered_objects=$(echo "$objects" | grep -v '/$')
            echo "$filtered_objects" >> "$OBJECTS_FILE"
            
            # 更新统计信息
            local batch_count=$(echo "$filtered_objects" | wc -l)
            count=$((count + batch_count))
            
            # 计算这批对象的总大小
            local batch_size_sum=$(echo "$filtered_objects" | awk '{sum+=$1} END {print sum}')
            total_size=$((total_size + batch_size_sum))
            
            log_message "已获取 $prefix 的 $batch_count 个对象，累计: $count"
        else
            log_message "前缀 '$prefix' 下没有对象或jq处理错误"
        fi
        
        # 检查是否有更多的对象
        local next_token=$(echo "$result" | jq -r '.NextToken // empty' 2>/dev/null)
        
        if [ -n "$next_token" ] && [ "$count" -lt "$SAMPLE_SIZE" ]; then
            marker="$next_token"
            log_message "使用续传标记继续获取对象: $marker"
        else
            continue=false
        fi
    done
    
    # 记录此前缀的统计信息
    if [ "$count" -gt 0 ]; then
        local avg_size=$(echo "scale=2; $total_size / $count" | bc)
        echo "$prefix|$count|$total_size|$avg_size" >> "$PREFIX_STATS_FILE"
        log_message "前缀 '$prefix' 统计: $count 个对象, 总大小: $total_size, 平均: $avg_size"
    fi
    
    echo -e "${GREEN}完成 ($count 个对象)${NC}"
}

# 获取子目录的函数
get_subdirectories() {
    local prefix="$1"
    local current_depth="$2"
    
    if [ "$current_depth" -gt "$DEPTH" ]; then
        log_message "达到最大深度 $DEPTH，停止递归"
        return
    fi
    
    log_message "正在获取前缀 '$prefix' 的子目录，当前深度: $current_depth"
    
    # 获取当前前缀下的子目录
    local delimiter_cmd="aws s3api list-objects-v2 --bucket \"$BUCKET\" --prefix \"$prefix\" --delimiter '/'"
    local subdirs=$(eval "$delimiter_cmd" | jq -r '.CommonPrefixes[]?.Prefix // empty' 2>/dev/null)
    
    # 记录调试信息
    local subdir_count=$(echo "$subdirs" | grep -v '^\s*$' | wc -l)
    log_message "前缀 '$prefix' 下发现 $subdir_count 个子目录"
    
    # 如果没有子目录，提前返回
    if [ -z "$subdirs" ]; then
        log_message "前缀 '$prefix' 下没有子目录"
        return
    fi
    
    # 处理每个子目录
    echo "$subdirs" | while read -r subdir; do
        if [ -z "$subdir" ]; then
            log_message "发现空子目录，跳过"
            continue
        fi
        
        # 获取子目录对象
        get_objects "$subdir"
        
        # 递归获取子目录的子目录
        get_subdirectories "$subdir" $((current_depth + 1))
    done
}

echo -e "${YELLOW}开始获取对象列表...${NC}"

# 先获取当前前缀下的对象
get_objects "$PREFIX"

# 递归获取子目录对象
echo -e "${YELLOW}正在递归获取子目录对象列表...${NC}"
get_subdirectories "$PREFIX" 1

# 检查是否获取到对象
if [ ! -s "$OBJECTS_FILE" ]; then
    echo -e "${RED}警告: 在prefix '$PREFIX'下未找到对象${NC}"
    echo -e "${BLUE}=== S3 Bucket分析结果 ===${NC}" > "$STATS_FILE"
    echo -e "总对象数: ${RED}0${NC}" >> "$STATS_FILE"
    echo -e "\n${YELLOW}建议: S3 bucket为空或指定的prefix下没有对象，无需进行Lustre条带化${NC}" >> "$STATS_FILE"
    cat "$STATS_FILE"
    echo -e "\n${BLUE}=== 分析完成 ===${NC}"
    echo -e "详细结果已保存到: ${GREEN}$STATS_FILE${NC}"
    exit 0
fi

# 分析目录结构
echo -e "${YELLOW}正在分析目录结构...${NC}"

# 提取所有目录前缀
echo -n "提取目录前缀..."
cat "$OBJECTS_FILE" | while read -r line; do
    key=$(echo "$line" | awk '{print $2}')
    dir=$(dirname "$key")
    if [ "$dir" != "." ]; then
        dir="${dir}/"
        for ((i=1; i<=DEPTH; i++)); do
            prefix=$(echo "$dir" | cut -d'/' -f1-$i | grep "/" | sed 's/[^/]*$//')
            if [ -n "$prefix" ]; then
                echo "$prefix"
            fi
        done
    fi
done | sort | uniq -c | sort -nr > "$PREFIXES_FILE"
echo -e "${GREEN}完成${NC}"

# 计算总对象数和总大小
echo -n "计算统计信息..."
TOTAL_OBJECTS=$(wc -l < "$OBJECTS_FILE")
TOTAL_SIZE=$(awk '{sum+=$1} END {print sum}' "$OBJECTS_FILE")
echo -e "${GREEN}完成${NC}"

# 计算平均文件大小
if [ "$TOTAL_OBJECTS" -gt 0 ]; then
    AVG_SIZE=$(echo "scale=2; $TOTAL_SIZE / $TOTAL_OBJECTS" | bc)
    AVG_SIZE_FORMATTED=$(format_size $(echo "$AVG_SIZE" | cut -d. -f1))
else
    AVG_SIZE_FORMATTED="0B"
fi

# 分析当前前缀下的文件数量（不含子目录文件）
if [ -z "$PREFIX" ]; then
    # 根目录特殊处理
    PREFIX_FILES=$(grep -c -v "/" "$OBJECTS_FILE")
    PREFIX_SIZE=$(grep -v "/" "$OBJECTS_FILE" | awk '{sum+=$1} END {print sum}')
else
    # 只计算直接在当前前缀下的文件，不包含子目录
    PREFIX_FILES=$(awk -v prefix="$PREFIX" '
        {
            key=$2;
            if (key ~ "^" prefix "[^/]*$") {
                count++;
            }
        }
        END {print count}
    ' "$OBJECTS_FILE")
    
    PREFIX_SIZE=$(awk -v prefix="$PREFIX" '
        {
            key=$2;
            if (key ~ "^" prefix "[^/]*$") {
                sum+=$1;
            }
        }
        END {print sum}
    ' "$OBJECTS_FILE")
fi

# 确保 PREFIX_FILES 和 PREFIX_SIZE 有默认值
if [ -z "$PREFIX_FILES" ]; then
    PREFIX_FILES=0
fi
if [ -z "$PREFIX_SIZE" ]; then
    PREFIX_SIZE=0
fi

# 计算当前前缀下的平均文件大小
if [ "$PREFIX_FILES" -gt 0 ] && [ "$PREFIX_SIZE" -gt 0 ]; then
    PREFIX_AVG_SIZE=$(echo "scale=2; $PREFIX_SIZE / $PREFIX_FILES" | bc)
    PREFIX_AVG_SIZE_FORMATTED=$(format_size $(echo "$PREFIX_AVG_SIZE" | cut -d. -f1))
else
    PREFIX_AVG_SIZE_FORMATTED="0B"
fi

log_message "总对象数: $TOTAL_OBJECTS, 总大小: $TOTAL_SIZE, 平均大小: $AVG_SIZE_FORMATTED"
log_message "当前前缀统计: 文件数: $PREFIX_FILES, 大小: $PREFIX_SIZE, 平均: $PREFIX_AVG_SIZE_FORMATTED"

# 生成分析报告
echo -e "${BLUE}=== S3目录结构分析结果 ===${NC}" > "$STATS_FILE"
echo -e "分析时间: $(date)" >> "$STATS_FILE"
echo -e "分析bucket: ${GREEN}$BUCKET${NC}" >> "$STATS_FILE"
echo -e "分析prefix: ${GREEN}$PREFIX${NC}" >> "$STATS_FILE"
echo -e "----------------------------------------" >> "$STATS_FILE"
echo -e "总对象数: ${GREEN}$TOTAL_OBJECTS${NC}" >> "$STATS_FILE"
echo -e "总大小: ${GREEN}$(format_size $TOTAL_SIZE)${NC}" >> "$STATS_FILE"
echo -e "平均文件大小: ${GREEN}$AVG_SIZE_FORMATTED${NC}" >> "$STATS_FILE"
echo -e "当前prefix统计:" >> "$STATS_FILE"
echo -e "  文件数: ${GREEN}$PREFIX_FILES${NC}" >> "$STATS_FILE"
echo -e "  总大小: ${GREEN}$(format_size $PREFIX_SIZE)${NC}" >> "$STATS_FILE"
echo -e "  平均文件大小: ${GREEN}$PREFIX_AVG_SIZE_FORMATTED${NC}" >> "$STATS_FILE"
echo -e "----------------------------------------" >> "$STATS_FILE"

# 输出顶级目录统计
echo -e "${BLUE}顶级目录统计:${NC}" >> "$STATS_FILE"
echo -e "${BLUE}目录\t\t\t文件数\t总大小\t平均大小${NC}" >> "$STATS_FILE"

# 直接使用已访问的前缀列表创建顶级目录
if [ -z "$PREFIX" ]; then
    # 根目录的特殊处理 - 找出所有一级前缀
    cat "$OBJECTS_FILE" | awk '{print $2}' | awk -F'/' '
        {
            if (NF > 1) {
                print $1 "/";
            }
        }
    ' | sort | uniq > "$TOP_DIRS_FILE"
    
    log_message "从对象列表中提取的根目录顶级目录数: $(wc -l < "$TOP_DIRS_FILE")"
else
    # 指定前缀下的顶级子目录
    cat "$OBJECTS_FILE" | awk -v prefix="$PREFIX" '
        {
            key=$2;
            if (key ~ "^" prefix) {
                # 移除前缀
                sub("^" prefix, "", key);
                # 提取第一级
                split(key, parts, "/");
                if (parts[1] != "" && index(key, "/") > 0) {
                    print prefix parts[1] "/";
                }
            }
        }
    ' | sort | uniq > "$TOP_DIRS_FILE"
    
    log_message "从对象列表中提取的前缀 $PREFIX 下顶级目录数: $(wc -l < "$TOP_DIRS_FILE")"
fi

# 输出目录统计信息
if [ -s "$TOP_DIRS_FILE" ]; then
    while read -r dir; do
        [ -z "$dir" ] && continue
        log_message "处理顶级目录: $dir"
        
        # 获取目录下的文件数和大小
        dir_files=$(grep -c "^[0-9][0-9]*[[:space:]]$dir" "$OBJECTS_FILE")
        dir_size=$(grep "^[0-9][0-9]*[[:space:]]$dir" "$OBJECTS_FILE" | awk '{sum+=$1} END {print sum}')
        
        log_message "目录 $dir 文件数: $dir_files, 大小: $dir_size"
        
        # 即使目录没有文件或大小为空，也显示它
        if [ -z "$dir_size" ]; then dir_size=0; fi
        if [ "$dir_files" -eq 0 ]; then
            printf "%-24s %8d %12s %12s\n" "$dir" 0 "0B" "0B" >> "$STATS_FILE"
        else
            avg_size=$(echo "scale=2; $dir_size / $dir_files" | bc)
            printf "%-24s %8d %12s %12s\n" "$dir" "$dir_files" "$(format_size $dir_size)" "$(format_size $(echo "$avg_size" | cut -d. -f1))" >> "$STATS_FILE"
        fi
    done < "$TOP_DIRS_FILE"
else
    echo -e "${YELLOW}没有找到子目录${NC}" >> "$STATS_FILE"
    log_message "没有找到顶级子目录"
fi

echo -e "----------------------------------------" >> "$STATS_FILE"

# 输出访问过的前缀统计 - 按文件数排序
echo -e "${BLUE}访问的前缀统计 (按文件数排序):${NC}" >> "$STATS_FILE"
echo -e "${BLUE}前缀\t\t\t文件数\t总大小\t平均大小${NC}" >> "$STATS_FILE"

# 按文件数排序前缀统计
if [ -s "$PREFIX_STATS_FILE" ]; then
    sort -t'|' -k2 -nr "$PREFIX_STATS_FILE" | head -20 | while IFS='|' read -r prefix count size avg; do
        [ -z "$prefix" ] && continue
        printf "%-24s %8d %12s %12s\n" "$prefix" "$count" "$(format_size $size)" "$(format_size $(echo "$avg" | cut -d. -f1))" >> "$STATS_FILE"
    done
else
    echo -e "${YELLOW}没有前缀统计信息${NC}" >> "$STATS_FILE"
fi

echo -e "----------------------------------------" >> "$STATS_FILE"

# 输出大型目录
echo -e "${BLUE}大型目录(文件数超过1000):${NC}" >> "$STATS_FILE"
echo -e "${BLUE}目录\t\t\t文件数\t总大小\t平均大小${NC}" >> "$STATS_FILE"

# 设置条带化阈值
FILE_THRESHOLD=1000
SIZE_THRESHOLD=1073741824  # 1GB

# 找出大型目录
if [ -s "$PREFIX_STATS_FILE" ]; then
    grep -v '^\s*$' "$PREFIX_STATS_FILE" | awk -F'|' -v threshold="$FILE_THRESHOLD" '$2 >= threshold {print $0}' | sort -t'|' -k2 -nr > "$OUTPUT_DIR/large_dirs.txt"
    LARGE_DIR_COUNT=$(wc -l < "$OUTPUT_DIR/large_dirs.txt")
else
    LARGE_DIR_COUNT=0
    touch "$OUTPUT_DIR/large_dirs.txt"
fi

log_message "识别的大型目录数: $LARGE_DIR_COUNT"

if [ "$LARGE_DIR_COUNT" -gt 0 ]; then
    # 使用表格格式输出大型目录
    cat "$OUTPUT_DIR/large_dirs.txt" | head -10 | while IFS='|' read -r prefix count size avg; do
        [ -z "$prefix" ] && continue
        printf "%-24s %8d %12s %12s\n" "$prefix" "$count" "$(format_size $size)" "$(format_size $(echo "$avg" | cut -d. -f1))" >> "$STATS_FILE"
    done
    
    # 如果大型目录超过10个，添加提示
    if [ "$LARGE_DIR_COUNT" -gt 10 ]; then
        echo -e "...等 $((LARGE_DIR_COUNT - 10)) 个目录 (共 $LARGE_DIR_COUNT 个大型目录)" >> "$STATS_FILE"
    fi
else
    echo -e "${YELLOW}没有找到大型子目录${NC}" >> "$STATS_FILE"
    log_message "没有找到大型子目录"
fi

echo -e "----------------------------------------" >> "$STATS_FILE"

# 条带化建议
echo -e "${BLUE}=== Lustre条带化建议 ===${NC}" >> "$STATS_FILE"

# 判断是否需要条带化
NEEDS_STRIPING=false
REASON=""

# 如果当前prefix下文件数量超过阈值
if [ "$PREFIX_FILES" -ge "$FILE_THRESHOLD" ]; then
    NEEDS_STRIPING=true
    REASON="当前prefix下包含$PREFIX_FILES个文件，超过阈值($FILE_THRESHOLD)"
    log_message "条带化原因: 当前前缀文件数超阈值"
fi

# 如果当前prefix的总大小超过阈值
if [ "$PREFIX_SIZE" -ge "$SIZE_THRESHOLD" ]; then
    NEEDS_STRIPING=true
    if [ -n "$REASON" ]; then
        REASON="$REASON；且当前prefix总大小$(format_size $PREFIX_SIZE)超过阈值($(format_size $SIZE_THRESHOLD))"
    else
        REASON="当前prefix总大小$(format_size $PREFIX_SIZE)超过阈值($(format_size $SIZE_THRESHOLD))"
    fi
    log_message "条带化原因: 当前前缀总大小超阈值"
fi

# 如果存在大型子目录
if [ "$LARGE_DIR_COUNT" -gt 0 ]; then
    NEEDS_STRIPING=true
    if [ -n "$REASON" ]; then
        REASON="$REASON；且存在$LARGE_DIR_COUNT个子目录，文件数超过阈值($FILE_THRESHOLD)"
    else
        REASON="存在$LARGE_DIR_COUNT个子目录，文件数超过阈值($FILE_THRESHOLD)"
    fi
    log_message "条带化原因: 存在大型子目录"
fi

# 如果总大小超过阈值
if [ "$TOTAL_SIZE" -ge "$SIZE_THRESHOLD" ]; then
    NEEDS_STRIPING=true
    if [ -n "$REASON" ]; then
        REASON="$REASON；且总大小$(format_size $TOTAL_SIZE)超过阈值($(format_size $SIZE_THRESHOLD))"
    else
        REASON="总大小$(format_size $TOTAL_SIZE)超过阈值($(format_size $SIZE_THRESHOLD))"
    fi
    log_message "条带化原因: 总大小超阈值"
fi

# 输出建议
if [ "$NEEDS_STRIPING" = true ]; then
    echo -e "${RED}建议: 考虑对以下目录进行条带化${NC}" >> "$STATS_FILE"
    echo -e "${RED}原因: $REASON${NC}" >> "$STATS_FILE"
    
    # 列出需要条带化的目录
    echo -e "\n${RED}推荐条带化的目录:${NC}" >> "$STATS_FILE"
    
    # 1. 当前前缀如果需要条带化
    if [ "$PREFIX_FILES" -ge "$FILE_THRESHOLD" ] || [ "$PREFIX_SIZE" -ge "$SIZE_THRESHOLD" ]; then
        current_prefix_display="$PREFIX"
        [ -z "$current_prefix_display" ] && current_prefix_display="(根目录)"
        echo -e "${RED}- $current_prefix_display${NC}" >> "$STATS_FILE"
        echo -e "  文件数: ${GREEN}$PREFIX_FILES${NC}, 总大小: ${GREEN}$(format_size $PREFIX_SIZE)${NC}, 平均: ${GREEN}$PREFIX_AVG_SIZE_FORMATTED${NC}" >> "$STATS_FILE"
    fi
    
    # 2. 大型子目录
    if [ "$LARGE_DIR_COUNT" -gt 0 ]; then
        cat "$OUTPUT_DIR/large_dirs.txt" | head -10 | while IFS='|' read -r prefix count size avg; do
            [ -z "$prefix" ] && continue
            # 跳过当前前缀（因为已经在上面处理过）
            if [ "$prefix" = "$PREFIX" ]; then
                continue
            fi
            echo -e "${RED}- $prefix${NC}" >> "$STATS_FILE"
            echo -e "  文件数: ${GREEN}$count${NC}, 总大小: ${GREEN}$(format_size $size)${NC}, 平均: ${GREEN}$(format_size $(echo "$avg" | cut -d. -f1))${NC}" >> "$STATS_FILE"
        done
        
        # 如果大型目录超过10个，添加提示
        if [ "$LARGE_DIR_COUNT" -gt 10 ]; then
            echo -e "${RED}- ...等 $((LARGE_DIR_COUNT - 10)) 个目录 (共 $LARGE_DIR_COUNT 个需条带化的目录)${NC}" >> "$STATS_FILE"
        fi
    fi
    
    # 输出条带化命令示例
    echo -e "\n${BLUE}条带化命令示例:${NC}" >> "$STATS_FILE"
    echo -e "# 在Lustre文件系统中创建父目录" >> "$STATS_FILE"
    if [ -z "$PREFIX" ]; then
        echo -e "mkdir -p /lustre/mount_point" >> "$STATS_FILE"
        echo -e "\n# 对整个目录进行条带化" >> "$STATS_FILE"
        echo -e "lfs setdirstripe -D -c -1 /lustre/mount_point" >> "$STATS_FILE"
    else
        target_dir=$(echo "$PREFIX" | sed 's|/$||')
        echo -e "mkdir -p /lustre/mount_point/$target_dir" >> "$STATS_FILE"
        echo -e "\n# 对特定目录进行条带化" >> "$STATS_FILE"
        echo -e "lfs setdirstripe -D -c -1 /lustre/mount_point/$target_dir" >> "$STATS_FILE"
    fi
    
    echo -e "\n# 对大型子目录进行条带化 (示例)" >> "$STATS_FILE"
    if [ "$LARGE_DIR_COUNT" -gt 0 ]; then
        cat "$OUTPUT_DIR/large_dirs.txt" | head -3 | while IFS='|' read -r prefix count size avg; do
            [ -z "$prefix" ] && continue
            # 跳过当前前缀
            if [ "$prefix" = "$PREFIX" ]; then
                continue
            fi
            target_dir=$(echo "$prefix" | sed 's|/$||')
            echo -e "mkdir -p /lustre/mount_point/$target_dir" >> "$STATS_FILE"
            echo -e "lfs setdirstripe -D -c -1 /lustre/mount_point/$target_dir" >> "$STATS_FILE"
        done
    fi
    
    echo -e "\n注: 条带化应在导入S3数据前执行，并且只对新创建的文件有效" >> "$STATS_FILE"
else
    echo -e "${GREEN}建议: 当前S3结构可能不需要特别的Lustre目录条带化${NC}" >> "$STATS_FILE"
    echo -e "${GREEN}原因: 目录结构简单或文件数量不足以显著受益于条带化${NC}" >> "$STATS_FILE"
fi

# 计算总执行时间
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))
echo -e "\n${BLUE}=== 执行统计 ===${NC}" >> "$STATS_FILE"
echo -e "总执行时间: ${GREEN}${MINUTES}分${SECONDS}秒${NC}" >> "$STATS_FILE"
echo -e "分析的对象数: ${GREEN}$TOTAL_OBJECTS${NC}" >> "$STATS_FILE"
echo -e "访问的目录数: ${GREEN}$(wc -l < "$VISITED_DIRS_FILE")${NC}" >> "$STATS_FILE"

# 输出结果
cat "$STATS_FILE"
echo -e "\n${BLUE}=== 分析完成 ===${NC}"
echo -e "详细结果已保存到: ${GREEN}$OUTPUT_DIR${NC}"
log_message "分析完成，执行时间: ${MINUTES}分${SECONDS}秒，分析的对象数: $TOTAL_OBJECTS"