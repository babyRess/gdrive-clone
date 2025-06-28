#!/bin/bash

# ==========================
# GOOGLE DRIVE CLONE - NEW SCRIPT WITH FOLDER ID
# ==========================
# Script sử dụng folder ID cho thư mục đích khi copy file

set -e  # Exit on error

# ==========================
# CONFIGURATION
# ==========================
# Thư mục nguồn
SRC="gdrive:Combo 8 Khóa Học Lập Trình của 200lab"
DEST="gdrive:"
# Thư mục đích (Folder ID)
DEST_FOLDER_ID="1njuuScxcdsafsdasdwww"
RCLONE_FLAGS="--drive-shared-with-me"

# Performance optimization settings
MAX_PARALLEL_JOBS=4  # Số job song song tối đa
TRANSFERS=8          # Số transfer đồng thời
CHECKERS=16          # Số checker đồng thời
BUFFER_SIZE="64M"    # Buffer size cho transfer
CHUNK_SIZE="128M"    # Chunk size cho upload lớn

# ==========================
# GLOBAL VARIABLES
# ==========================
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="clone_new_${TIMESTAMP}.log"
RESTRICTED_FILES="restricted_${TIMESTAMP}.txt"
TEMP_BASE_DIR="/tmp/rclone_clone_$$"

# Tạo associative array để lưu folder ID mapping
declare -A FOLDER_ID_MAP

# Tạo thư mục temp chính
mkdir -p "$TEMP_BASE_DIR"

# Cleanup function để xóa tất cả temp files khi script kết thúc
cleanup() {
    if [ -d "$TEMP_BASE_DIR" ]; then
        log "🧹 Cleaning up temporary files..."
        rm -rf "$TEMP_BASE_DIR"
        success "Temporary files cleaned up"
    fi
}

# Đăng ký cleanup function để chạy khi script exit
trap cleanup EXIT

# ==========================
# UTILITY FUNCTIONS
# ==========================

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ❌ ERROR: $1" | tee -a "$LOG_FILE"
}

warning() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ⚠️ WARNING: $1" | tee -a "$LOG_FILE"
}

success() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ✅ SUCCESS: $1" | tee -a "$LOG_FILE"
}

# Lấy folder ID từ đường dẫn và cache lại
get_folder_id() {
    local dir_path="$1"

    # Nếu là thư mục gốc
    if [ -z "$dir_path" ] || [ "$dir_path" = "." ]; then
        echo "$DEST_FOLDER_ID"
        return 0
    fi

    # Kiểm tra cache trước
    if [ -n "${FOLDER_ID_MAP[$dir_path]}" ]; then
        echo "${FOLDER_ID_MAP[$dir_path]}"
        return 0
    fi

    # Tách đường dẫn thành các phần và tìm từng cấp
    local current_path=""
    local current_folder_id="$DEST_FOLDER_ID"
    local IFS="/"

    for part in $dir_path; do
        if [ -n "$part" ]; then
            if [ -n "$current_path" ]; then
                current_path="$current_path/$part"
            else
                current_path="$part"
            fi

            # Kiểm tra cache cho đường dẫn hiện tại
            if [ -n "${FOLDER_ID_MAP[$current_path]}" ]; then
                current_folder_id="${FOLDER_ID_MAP[$current_path]}"
                continue
            fi

            # Tìm folder ID của phần hiện tại
            local folder_id
            folder_id=$(rclone lsjson "$DEST" --drive-root-folder-id="$current_folder_id" --dirs-only --max-depth 1 | jq -r ".[] | select(.Name == \"$part\") | .ID" 2>/dev/null)

            if [ -n "$folder_id" ] && [ "$folder_id" != "null" ]; then
                current_folder_id="$folder_id"
                # Cache lại
                FOLDER_ID_MAP["$current_path"]="$folder_id"
            else
                # Không tìm thấy, trả về folder ID hiện tại
                echo "$current_folder_id"
                return 1
            fi
        fi
    done

    echo "$current_folder_id"
    return 0
}

# Cache cho file listing để tránh query lại
declare -A FILE_LIST_CACHE

# Kiểm tra file đã tồn tại bằng folder ID (với cache)
file_exists() {
    local dir_path="$1"
    local filename="$2"

    local folder_id
    folder_id=$(get_folder_id "$dir_path")

    # Kiểm tra cache trước
    local cache_key="$folder_id"
    if [ -z "${FILE_LIST_CACHE[$cache_key]}" ]; then
        # Cache file list cho folder này
        FILE_LIST_CACHE[$cache_key]=$(rclone lsf "$DEST" --drive-root-folder-id="$folder_id" --fast-list 2>/dev/null | tr '\n' '|')
    fi

    # Kiểm tra file trong cache
    if echo "${FILE_LIST_CACHE[$cache_key]}" | grep -q "|$filename|"; then
        return 0
    fi
    return 1
}

# Kiểm tra thư mục đã tồn tại
dir_exists() {
    local dir_path="$1"

    if [ -z "$dir_path" ] || [ "$dir_path" = "." ]; then
        return 0
    fi

    # Thử lấy folder ID, nếu thành công thì thư mục tồn tại
    local folder_id
    folder_id=$(get_folder_id "$dir_path")

    if [ $? -eq 0 ] && [ -n "$folder_id" ]; then
        return 0
    fi
    return 1
}

# ==========================
# CORE FUNCTIONS
# ==========================

# Tạo thư mục và lưu folder ID
create_directory() {
    local dir_path="$1"

    if dir_exists "$dir_path"; then
        log "⏭️ Directory already exists: $dir_path"
        # Nếu thư mục đã tồn tại, vẫn cần cache folder ID
        get_folder_id "$dir_path" > /dev/null
        return 0
    fi

    log "📁 Creating directory: $dir_path"

    # Tạo từng cấp thư mục
    local current_path=""
    local current_folder_id="$DEST_FOLDER_ID"
    local IFS="/"

    for part in $dir_path; do
        if [ -n "$part" ]; then
            if [ -n "$current_path" ]; then
                current_path="$current_path/$part"
            else
                current_path="$part"
            fi

            # Kiểm tra xem thư mục con này đã tồn tại chưa
            if [ -n "${FOLDER_ID_MAP[$current_path]}" ]; then
                current_folder_id="${FOLDER_ID_MAP[$current_path]}"
                continue
            fi

            # Kiểm tra thư mục đã tồn tại
            local existing_id
            existing_id=$(rclone lsjson "$DEST" --drive-root-folder-id="$current_folder_id" --dirs-only --max-depth 1 | jq -r ".[] | select(.Name == \"$part\") | .ID" 2>/dev/null)

            if [ -n "$existing_id" ] && [ "$existing_id" != "null" ]; then
                # Thư mục đã tồn tại
                current_folder_id="$existing_id"
                FOLDER_ID_MAP["$current_path"]="$existing_id"
                log "📋 Found existing folder ID for '$current_path': $existing_id"
            else
                # Tạo thư mục mới
                if rclone mkdir "$DEST/$current_path" --drive-root-folder-id="$DEST_FOLDER_ID"; then
                    sleep 1  # Chờ Google Drive xử lý

                    # Lấy folder ID mới tạo
                    local new_folder_id
                    new_folder_id=$(rclone lsjson "$DEST" --drive-root-folder-id="$current_folder_id" --dirs-only --max-depth 1 | jq -r ".[] | select(.Name == \"$part\") | .ID" 2>/dev/null)

                    if [ -n "$new_folder_id" ] && [ "$new_folder_id" != "null" ]; then
                        current_folder_id="$new_folder_id"
                        FOLDER_ID_MAP["$current_path"]="$new_folder_id"
                        log "📋 Created and cached folder ID for '$current_path': $new_folder_id"
                    else
                        error "Failed to get folder ID for newly created directory: $current_path"
                        return 1
                    fi
                else
                    error "Failed to create directory: $current_path"
                    return 1
                fi
            fi
        fi
    done

    success "Directory structure ready: $dir_path"
    return 0
}

# Download stream cho file bị hạn chế (tối ưu tốc độ)
download_stream() {
    local src_file="$1"
    local dest_dir="$2"
    local filename="$3"

    log "🌊 Attempting high-speed download stream: $src_file"

    # Lấy folder ID của thư mục đích
    local dest_folder_id
    dest_folder_id=$(get_folder_id "$dest_dir")

    # Phương pháp 1: Stream trực tiếp với tối ưu (không cần temp file)
    if rclone cat "$SRC/$src_file" $RCLONE_FLAGS \
        --buffer-size="$BUFFER_SIZE" \
        --transfers="$TRANSFERS" 2>/dev/null | \
       rclone rcat "$DEST/$filename" \
        --drive-root-folder-id="$dest_folder_id" \
        --buffer-size="$BUFFER_SIZE" \
        --drive-chunk-size="$CHUNK_SIZE" 2>/dev/null; then
        success "High-speed stream successful: $dest_dir/$filename"
        return 0
    fi

    # Phương pháp 2: Download qua temp với tối ưu
    local temp_dir="$TEMP_BASE_DIR/stream_$(date +%s)_$$"
    mkdir -p "$temp_dir"

    log "📥 High-speed download to temp: $temp_dir"

    if rclone copy "$SRC/$src_file" "$temp_dir" $RCLONE_FLAGS \
        --transfers="$TRANSFERS" \
        --checkers="$CHECKERS" \
        --buffer-size="$BUFFER_SIZE" \
        --ignore-errors 2>/dev/null; then

        if [ -f "$temp_dir/$filename" ]; then
            log "📤 High-speed upload from temp"
            if rclone copy "$temp_dir/$filename" "$DEST" \
                --drive-root-folder-id="$dest_folder_id" \
                --transfers="$TRANSFERS" \
                --buffer-size="$BUFFER_SIZE" \
                --drive-chunk-size="$CHUNK_SIZE" 2>/dev/null; then

                success "High-speed temp transfer successful: $dest_dir/$filename"
                log "🧹 Cleaning up temp file: $temp_dir"
                rm -rf "$temp_dir"
                return 0
            fi
        fi
    fi

    log "🧹 Cleaning up failed temp download: $temp_dir"
    rm -rf "$temp_dir"
    return 1
}

# Copy file sử dụng folder ID
copy_file() {
    local src_file="$1"
    local dest_dir="$2"
    local filename="$3"

    log "📄 Copying file: $src_file -> $dest_dir/$filename"

    # Lấy folder ID của thư mục đích
    local dest_folder_id
    dest_folder_id=$(get_folder_id "$dest_dir")

    if [ $? -ne 0 ] || [ -z "$dest_folder_id" ]; then
        error "Cannot get folder ID for: $dest_dir"
        # Thử tạo lại thư mục
        warning "Attempting to recreate directory structure..."
        if create_directory "$dest_dir"; then
            dest_folder_id=$(get_folder_id "$dest_dir")
            if [ $? -ne 0 ] || [ -z "$dest_folder_id" ]; then
                error "Still cannot get folder ID after recreation: $dest_dir"
                return 1
            fi
        else
            error "Failed to recreate directory: $dest_dir"
            return 1
        fi
    fi

    log "📋 Using destination folder ID: $dest_folder_id"

    # Kiểm tra file đã tồn tại (sau khi có folder ID)
    if file_exists "$dest_dir" "$filename"; then
        log "⏭️ File already exists: $dest_dir/$filename"
        return 0
    fi

    # Thử copy server-side với tối ưu tốc độ
    local copy_result
    copy_result=$(rclone copy "$SRC/$src_file" "$DEST" \
        $RCLONE_FLAGS \
        --drive-root-folder-id="$dest_folder_id" \
        --drive-server-side-across-configs=true \
        --transfers="$TRANSFERS" \
        --checkers="$CHECKERS" \
        --buffer-size="$BUFFER_SIZE" \
        --drive-chunk-size="$CHUNK_SIZE" \
        --fast-list \
        --retries 1 \
        2>&1)

    # Kiểm tra lỗi 403
    if echo "$copy_result" | grep -q -E "(cannotCopyFile|cannotDownloadFile)"; then
        warning "Permission error detected, trying download stream..."

        if download_stream "$src_file" "$dest_dir" "$filename"; then
            return 0
        else
            error "All methods failed for: $src_file"
            echo "$src_file" >> "$RESTRICTED_FILES"
            return 1
        fi
    fi

    # Kiểm tra kết quả
    sleep 2
    if file_exists "$dest_dir" "$filename"; then
        success "File copied successfully: $dest_dir/$filename"
        return 0
    else
        warning "Server-side copy failed, trying download stream..."
        if download_stream "$src_file" "$dest_dir" "$filename"; then
            return 0
        else
            error "All methods failed for: $src_file"
            echo "$src_file" >> "$RESTRICTED_FILES"
            return 1
        fi
    fi
}

# ==========================
# MAIN PROCESS
# ==========================

main() {
    log "🚀 Starting Google Drive Clone (New Script with Folder ID)"
    log "Source: $SRC"
    log "Destination: $DEST (Folder ID: $DEST_FOLDER_ID)"

    # Kiểm tra dependencies
    for cmd in rclone jq; do
        if ! command -v "$cmd" &> /dev/null; then
            error "Missing dependency: $cmd"
            exit 1
        fi
    done

    # Kiểm tra quyền truy cập
    log "🔍 Checking access to destination..."
    if ! rclone lsd "$DEST" --drive-root-folder-id="$DEST_FOLDER_ID" &>/dev/null; then
        error "Cannot access destination folder ID: $DEST_FOLDER_ID"
        exit 1
    fi

    # PHASE 1: Tạo cấu trúc thư mục và cache folder ID
    log "📁 PHASE 1: Creating directory structure and caching folder IDs..."

    # Lấy danh sách tất cả thư mục
    log "🔍 Scanning directories..."
    local dirs
    dirs=$(rclone lsjson -R "$SRC" $RCLONE_FLAGS --dirs-only | jq -r '.[].Path' | sort)

    local dir_count=0
    local dir_created=0
    local dir_skipped=0

    while IFS= read -r dir; do
        if [ -n "$dir" ]; then
            dir_count=$((dir_count + 1))
            if create_directory "$dir"; then
                if dir_exists "$dir"; then
                    dir_created=$((dir_created + 1))
                fi
            else
                dir_skipped=$((dir_skipped + 1))
            fi
        fi
    done <<< "$dirs"

    log "📊 Directory phase completed: $dir_count total, $dir_created created, $dir_skipped skipped"
    log "📋 Cached ${#FOLDER_ID_MAP[@]} folder IDs for efficient file copying"

    # PHASE 2: Copy files using cached folder IDs
    log "📄 PHASE 2: Copying files using folder IDs..."

    local file_count=0
    local file_copied=0
    local file_skipped=0
    local file_failed=0

    # Copy files trong thư mục gốc
    log "📁 Processing root directory files..."
    local root_files
    root_files=$(rclone lsjson "$SRC" $RCLONE_FLAGS --files-only --fast-list | jq -r '.[].Name')

    local root_file_count=0
    local root_file_copied=0
    local root_file_failed=0

    # Đếm file trong thư mục gốc
    while IFS= read -r file; do
        if [ -n "$file" ]; then
            root_file_count=$((root_file_count + 1))
        fi
    done <<< "$root_files"

    log "📊 Found $root_file_count files in root directory"

    # Copy từng file trong thư mục gốc
    local current_root_file=0
    while IFS= read -r file; do
        if [ -n "$file" ]; then
            current_root_file=$((current_root_file + 1))
            file_count=$((file_count + 1))

            log "📄 Processing root file ($current_root_file/$root_file_count): $file"

            if copy_file "$file" "" "$file"; then
                file_copied=$((file_copied + 1))
                root_file_copied=$((root_file_copied + 1))
                success "Root file copied: $file"
            else
                file_failed=$((file_failed + 1))
                root_file_failed=$((root_file_failed + 1))
                error "Root file failed: $file"
            fi
        fi
    done <<< "$root_files"

    log "📊 Root directory summary: $root_file_copied copied, $root_file_failed failed out of $root_file_count total"

    # Copy files trong các thư mục con (sequential để đảm bảo reliability)
    log "� Processing files in subdirectories..."

    local total_dirs=0
    local processed_dirs=0

    # Đếm tổng số thư mục
    while IFS= read -r dir; do
        if [ -n "$dir" ]; then
            total_dirs=$((total_dirs + 1))
        fi
    done <<< "$dirs"

    # Xử lý từng thư mục
    while IFS= read -r dir; do
        if [ -n "$dir" ]; then
            processed_dirs=$((processed_dirs + 1))
            log "📁 Processing directory ($processed_dirs/$total_dirs): $dir"

            # Lấy danh sách file trong thư mục với fast-list
            local dir_files
            dir_files=$(rclone lsjson "$SRC/$dir" $RCLONE_FLAGS --files-only --fast-list | jq -r '.[].Name')

            local dir_file_count=0
            local dir_file_copied=0
            local dir_file_failed=0

            # Đếm file trong thư mục
            while IFS= read -r file; do
                if [ -n "$file" ]; then
                    dir_file_count=$((dir_file_count + 1))
                fi
            done <<< "$dir_files"

            log "📊 Found $dir_file_count files in directory: $dir"

            # Copy từng file trong thư mục
            local current_file=0
            while IFS= read -r file; do
                if [ -n "$file" ]; then
                    current_file=$((current_file + 1))
                    file_count=$((file_count + 1))

                    log "📄 Processing file ($current_file/$dir_file_count): $file"

                    if copy_file "$dir/$file" "$dir" "$file"; then
                        file_copied=$((file_copied + 1))
                        dir_file_copied=$((dir_file_copied + 1))
                        success "File copied: $dir/$file"
                    else
                        file_failed=$((file_failed + 1))
                        dir_file_failed=$((dir_file_failed + 1))
                        error "File failed: $dir/$file"
                    fi

                    # Progress report mỗi 10 file
                    if [ $((current_file % 10)) -eq 0 ]; then
                        log "📊 Progress: $current_file/$dir_file_count files processed in $dir"
                    fi
                fi
            done <<< "$dir_files"

            log "📊 Directory summary for '$dir': $dir_file_copied copied, $dir_file_failed failed out of $dir_file_count total"

            # Invalidate cache cho thư mục này sau khi copy xong
            local folder_id=$(get_folder_id "$dir" 2>/dev/null)
            if [ -n "$folder_id" ]; then
                unset FILE_LIST_CACHE["$folder_id"]
            fi
        fi
    done <<< "$dirs"

    # Tổng kết
    log "🎉 PROCESS COMPLETED!"
    log "📊 FINAL STATISTICS:"
    log "  - Directories: $dir_count total, $dir_created created"
    log "  - Files: $file_count total, $file_copied copied, $file_failed failed"
    log "  - Folder IDs cached: ${#FOLDER_ID_MAP[@]}"

    local restricted_count=0
    if [ -f "$RESTRICTED_FILES" ]; then
        restricted_count=$(wc -l < "$RESTRICTED_FILES" 2>/dev/null || echo "0")
    fi

    if [ $restricted_count -gt 0 ]; then
        warning "$restricted_count files were restricted and saved to: $RESTRICTED_FILES"
    fi

    if [ $file_failed -eq 0 ]; then
        success "All files copied successfully!"
    else
        warning "Some files failed to copy. Check log for details."
    fi

    log "📋 Log file: $LOG_FILE"

    # Hiển thị một số folder ID đã cache (để debug)
    if [ ${#FOLDER_ID_MAP[@]} -gt 0 ]; then
        log "📋 Sample cached folder IDs:"
        local count=0
        for dir_path in "${!FOLDER_ID_MAP[@]}"; do
            if [ $count -lt 5 ]; then
                log "  '$dir_path' -> ${FOLDER_ID_MAP[$dir_path]}"
                count=$((count + 1))
            fi
        done
    fi

    # Kiểm tra và báo cáo temp files
    if [ -d "$TEMP_BASE_DIR" ]; then
        local temp_size=$(du -sh "$TEMP_BASE_DIR" 2>/dev/null | cut -f1)
        if [ -n "$temp_size" ]; then
            log "📊 Temporary files size: $temp_size"
        fi

        local temp_count=$(find "$TEMP_BASE_DIR" -type f 2>/dev/null | wc -l)
        if [ "$temp_count" -gt 0 ]; then
            warning "$temp_count temporary files still exist (will be cleaned up on exit)"
        else
            log "✅ No temporary files remaining"
        fi
    fi

    log "🏁 Script completed at $(date)"
    log "🧹 Temporary files will be automatically cleaned up on exit"
}

# Chạy script
main "$@"