echo "开始打包"

source ../../../venv/Scripts/activate

VERSION="0.1.0"

python -m nuitka --standalone --onefile --windows-icon-from-ico=web/favicon.ico \
    --file-version=$VERSION --product-version=$VERSION \
    --include-data-files=templates/index.html=templates/index.html  --include-data-dir=web=web \
    --output-dir=nuitka_build main.py


echo "打包完成"
