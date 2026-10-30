#!/bin/bash
# 一键打三平台单文件版本
set -e
cd "$(dirname "$0")/goapp"
export PATH="$HOME/sdk/go/bin:$PATH"
export CGO_ENABLED=0

BT=$(date +%Y%m%d-%H%M%S)
# 版本号只有 goapp/main.go 一个来源，别在脚本里再写死一份
APPVER="$(sed -n 's/^const appVersion = "\(.*\)".*$/\1/p' main.go | head -1)"
if [ -z "$APPVER" ]; then
  echo "读不到 goapp/main.go 里的 const appVersion，检查那行是不是被改过" >&2
  exit 1
fi
LDX="-s -w -X main.buildTime=$BT"
# 自建分发时用 UPDATE_URL=https://你的地址/ ./build.sh 注入；不设则为空＝不检查更新
if [ -n "${UPDATE_URL:-}" ]; then LDX="$LDX -X main.defaultUpdateURL=$UPDATE_URL"; fi
FLAGS=(-trimpath -ldflags "$LDX")

echo "→ windows/amd64（无控制台窗口）"
GOOS=windows GOARCH=amd64 go build -trimpath -ldflags "-s -w -H windowsgui -X main.buildTime=$BT${UPDATE_URL:+ -X main.defaultUpdateURL=$UPDATE_URL}" -o ../release-win/medupdate.exe .

echo "→ darwin/arm64"
GOOS=darwin GOARCH=arm64 go build "${FLAGS[@]}" -o ../release-mac/medupdate .

echo "→ linux/amd64"
GOOS=linux GOARCH=amd64 go build "${FLAGS[@]}" -o ../release-linux/medupdate .

# 本机运行用的副本（launchd 指向它）
GOOS=darwin GOARCH=arm64 go build "${FLAGS[@]}" -o ../medupdate .

# macOS App 包里的二进制也要更新，否则双击 app 跑的还是老代码
echo "→ 更新 macOS App 包"
APP="${APP_BUNDLE:-../dist/update-tool.app}"
# 没指定且默认目录不存在时，退而使用 dist 下任意一个 .app，
# 避免本机改名后这里静默跳过、双击跑的还是老代码
if [ ! -d "$APP" ]; then
  for cand in ../dist/*.app; do
    [ -d "$cand" ] && APP="$cand" && break
  done
fi
if [ -d "$APP" ]; then
  cp ../release-mac/medupdate "$APP/Contents/MacOS/medupdate"
  chmod +x "$APP/Contents/MacOS/medupdate"
  /usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString $APPVER" "$APP/Contents/Info.plist" >/dev/null
  codesign --force --deep -s - "$APP" >/dev/null 2>&1 || true
  echo "   app 已同步（v$APPVER）"
fi

echo "$BT" > ../build_id.txt
ls -lh ../release-win/medupdate.exe ../release-mac/medupdate ../release-linux/medupdate
if [ -d "$APP" ]; then ls -l "$APP/Contents/MacOS/medupdate"; fi
