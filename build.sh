#!/bin/bash
# 一键打三平台单文件版本
set -e
cd "$(dirname "$0")/goapp"
export PATH="$HOME/sdk/go/bin:$PATH"
export CGO_ENABLED=0

BT=$(date +%Y%m%d-%H%M%S)
FLAGS=(-trimpath -ldflags "-s -w -X main.buildTime=$BT")

echo "→ windows/amd64（无控制台窗口）"
GOOS=windows GOARCH=amd64 go build -trimpath -ldflags "-s -w -H windowsgui -X main.buildTime=$BT" -o ../release-win/medupdate.exe .

echo "→ darwin/arm64"
GOOS=darwin GOARCH=arm64 go build "${FLAGS[@]}" -o ../release-mac/medupdate .

echo "→ linux/amd64"
GOOS=linux GOARCH=amd64 go build "${FLAGS[@]}" -o ../release-linux/medupdate .

# 本机运行用的副本（launchd 指向它）
GOOS=darwin GOARCH=arm64 go build "${FLAGS[@]}" -o ../medupdate .

# macOS App 包里的二进制也要更新，否则双击 app 跑的还是老代码
echo "→ 更新 macOS App 包"
APP="../dist/update-tool.app"
if [ -d "$APP" ]; then
  cp ../release-mac/medupdate "$APP/Contents/MacOS/medupdate"
  chmod +x "$APP/Contents/MacOS/medupdate"
  /usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString 1.0.1" "$APP/Contents/Info.plist" >/dev/null 2>&1 || true
  codesign --force --deep -s - "$APP" >/dev/null 2>&1 || true
  echo "   app 已同步"
fi

echo "$BT" > ../build_id.txt
ls -lh ../release-win/medupdate.exe ../release-mac/medupdate ../release-linux/medupdate
if [ -d "$APP" ]; then ls -l "$APP/Contents/MacOS/medupdate"; fi
