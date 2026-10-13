
from flask import Flask, render_template, jsonify, request, send_file
from urllib.parse import quote
import json
import os
import pandas as pd
import numpy as np

app = Flask(__name__, 
            template_folder='templates',
            static_folder='static')

# 尝试导入 analyze_funds 相关功能，如果失败则使用模拟数据
try:
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from analyze_funds import get_excess_return_curve_for_fund
    USE_REAL_DATA = True
    print('成功导入 analyze_funds，将使用真实数据')
except Exception as e:
    USE_REAL_DATA = False
    print(f'导入 analyze_funds 失败: {e}，将使用模拟数据')

# 尝试导入服务端 PDF 生成所需的库（备用方案）
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from io import BytesIO
    PDF_SERVER_AVAILABLE = True
    # 尝试注册中文字体
    try:
        pdfmetrics.registerFont(TTFont('SimSun', r'C:\Windows\Fonts\simsun.ttc'))
        PDF_FONT = 'SimSun'
    except Exception:
        try:
            pdfmetrics.registerFont(TTFont('ArialUnicode', 'Arial Unicode.ttf'))
            PDF_FONT = 'ArialUnicode'
        except Exception:
            PDF_FONT = 'Helvetica'
except ImportError:
    PDF_SERVER_AVAILABLE = False
    print('警告: 未安装 reportlab，服务端PDF导出不可用（前端导出仍然可用）')
    print('如需启用服务端方案: pip install reportlab')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/fund_ranking')
def fund_ranking():
    from enhanced_index import SELECTED_FUND_TYPES
    return render_template('fund_ranking.html', selected_fund_types=SELECTED_FUND_TYPES)

@app.route('/chart_image')
def chart_image():
    """提供超额收益曲线图片"""
    fund_type = request.args.get('fund_type', '')
    if not fund_type:
        return '', 404
    filename = f'{fund_type}基金_超额曲线.png'
    if os.path.exists(filename):
        return send_file(filename, mimetype='image/png')
    return '', 404

@app.route('/chart_data')
def chart_data():
    """提供超额收益曲线JSON数据，供前端ECharts渲染"""
    fund_type = request.args.get('fund_type', '')
    if not fund_type:
        return jsonify({'code': 1, 'msg': '缺少fund_type参数'})
    json_filename = f'{fund_type}基金_超额曲线.json'
    if not os.path.exists(json_filename):
        return jsonify({'code': 1, 'msg': '曲线数据不存在，请先运行 enhanced_index.py'})
    try:
        with open(json_filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify({'code': 0, 'data': data})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

@app.route('/get_fund_data')
def get_fund_data():
    """从 index-fund.xlsx 读取指定工作表的数据"""
    sheet = request.args.get('sheet', '')
    if not sheet:
        return jsonify({'code': 1, 'msg': '缺少sheet参数', 'count': 0, 'data': []})

    limit = request.args.get('limit', type=int)
    sort_by = request.args.get('sort_by', '近1年超额')

    EXCEL_FILE = 'index-fund.xlsx'
    if not os.path.exists(EXCEL_FILE):
        return jsonify({'code': 1, 'msg': 'Excel文件不存在', 'count': 0, 'data': []})

    # 工作表不存在（如大盘主动等尚未生成数据的类型）时返回干净的空数据，
    # 避免 pd.read_excel 抛出 "Worksheet not found" 被当作错误透传给前端
    try:
        if sheet not in pd.ExcelFile(EXCEL_FILE).sheet_names:
            response_data = {'code': 0, 'msg': '暂无数据', 'count': 0, 'data': []}
            return app.response_class(
                response=json.dumps(response_data, ensure_ascii=False),
                status=200,
                mimetype='application/json'
            )
    except Exception:
        pass

    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name=sheet)
        df = df.replace({np.nan: None})

        # 处理基金代码，确保为6位字符串格式
        if '基金代码' in df.columns:
            df['基金代码'] = df['基金代码'].apply(
                lambda x: str(int(x)).zfill(6) if isinstance(x, (int, float)) and not pd.isna(x) else str(x).zfill(6) if x is not None else ''
            )

        # 处理日期类型
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)

        # 排序并截取前 N 条
        if limit and limit > 0:
            if sort_by in df.columns:
                df[sort_by] = pd.to_numeric(df[sort_by], errors='coerce')
                df = df.sort_values(by=sort_by, ascending=False).head(limit)
            else:
                df = df.head(limit)

        total = len(df)
        data = df.to_dict('records')

        response_data = {
            'code': 0,
            'msg': 'success',
            'count': total,
            'data': data
        }
        response_json = json.dumps(response_data, ensure_ascii=False)
        return app.response_class(
            response=response_json,
            status=200,
            mimetype='application/json'
        )
    except Exception as e:
        response_data = {'code': 1, 'msg': str(e), 'count': 0, 'data': []}
        response_json = json.dumps(response_data, ensure_ascii=False)
        return app.response_class(
            response=response_json,
            status=200,
            mimetype='application/json'
        )

@app.route('/quant_fund_ranking')
def quant_fund_ranking():
    return render_template('quant_fund_ranking.html')


@app.route('/get_quant_fund_data')
def get_quant_fund_data():
    EXCEL_FILE = 'fund_open_fund_rank_em.xlsx'
    
    if not os.path.exists(EXCEL_FILE):
        return jsonify({'code': 1, 'msg': 'Excel文件不存在', 'count': 0, 'data': []})
    
    try:
        df = pd.read_excel(EXCEL_FILE)
        df = df.replace({np.nan: None})
        
        # 处理基金代码，确保为6位字符串格式
        if '基金代码' in df.columns:
            df['基金代码'] = df['基金代码'].apply(
                lambda x: str(int(x)).zfill(6) if isinstance(x, (int, float)) and not pd.isna(x) else str(x).zfill(6) if x is not None else ''
            )
        
        # 处理所有日期/时间类型，转换为字符串
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
            elif pd.api.types.is_timedelta64_dtype(df[col]):
                df[col] = df[col].astype(str)
        
        total = len(df)
        data = df.to_dict('records')
        
        response_data = {
            'code': 0,
            'msg': 'success',
            'count': total,
            'data': data
        }
        
        response_json = json.dumps(response_data, ensure_ascii=False)
        return app.response_class(
            response=response_json,
            status=200,
            mimetype='application/json'
        )
    
    except Exception as e:
        response_data = {'code': 1, 'msg': str(e), 'count': 0, 'data': []}
        response_json = json.dumps(response_data, ensure_ascii=False)
        return app.response_class(
            response=response_json,
            status=200,
            mimetype='application/json'
        )

@app.route('/get_excess_curve/<fund_code>')
def get_excess_curve(fund_code):
    print(f"\n=== 请求超额收益: {fund_code} ===")
    try:
        # 首先尝试从 Excel 文件中读取已有的数据
        EXCEL_FILE = 'fund_open_fund_rank_em.xlsx'
        if os.path.exists(EXCEL_FILE):
            try:
                df = pd.read_excel(EXCEL_FILE)
                # 处理基金代码格式，确保匹配
                if '基金代码' in df.columns:
                    df['基金代码'] = df['基金代码'].apply(
                        lambda x: str(int(x)).zfill(6) if isinstance(x, (int, float)) and not pd.isna(x) else str(x).zfill(6) if x is not None else ''
                    )
                    # 查找对应的基金
                    fund_row = df[df['基金代码'] == fund_code]
                    if len(fund_row) > 0 and '超额收益曲线' in df.columns:
                        curve_json = fund_row.iloc[0]['超额收益曲线']
                        if pd.notna(curve_json) and curve_json:
                            # 尝试解析 JSON
                            try:
                                if isinstance(curve_json, str):
                                    excess_curve = json.loads(curve_json)
                                else:
                                    excess_curve = curve_json
                                if excess_curve and isinstance(excess_curve, list) and len(excess_curve) > 0:
                                    print(f"从 Excel 成功读取数据: {len(excess_curve)} 个点")
                                    if len(excess_curve) > 0:
                                        first = excess_curve[0]
                                        last = excess_curve[-1]
                                        print(f"  起始: {first['date']} = {first['excess_return']*100:.2f}%")
                                        print(f"  结束: {last['date']} = {last['excess_return']*100:.2f}%")
                                
                                result = {'code': 0, 'data': excess_curve}
                                if '近1年' in df.columns:
                                    result['annual_return'] = float(fund_row.iloc[0]['近1年']) if pd.notna(fund_row.iloc[0]['近1年']) else None
                                return jsonify(result)
                            except Exception as e:
                                print(f"解析 Excel 中的超额收益曲线失败: {e}")
            except Exception as e:
                print(f"从 Excel 读取失败: {e}")
        
        # Excel 中没有，才重新获取
        print("Excel 中无数据，重新获取...")
        if USE_REAL_DATA:
            excess_curve = get_excess_return_curve_for_fund(fund_code, days=365)
            if excess_curve and isinstance(excess_curve, list) and len(excess_curve) > 0:
                print(f"成功获取真实数据: {len(excess_curve)} 个点")
                if len(excess_curve) > 0:
                    first = excess_curve[0]
                    last = excess_curve[-1]
                    print(f"  起始: {first['date']} = {first['excess_return']*100:.2f}%")
                    print(f"  结束: {last['date']} = {last['excess_return']*100:.2f}%")
                return jsonify({'code': 0, 'data': excess_curve})
            else:
                print("真实数据为空，返回空数据")
                return jsonify({'code': 0, 'data': []})
        else:
            print("USE_REAL_DATA = False，返回空数据")
            return jsonify({'code': 0, 'data': []})
    except Exception as e:
        print(f'获取超额收益曲线失败: {e}')
        import traceback
        traceback.print_exc()
        # 出错时返回空数据
        return jsonify({'code': 0, 'data': []})

@app.route('/export_pdf')
def export_pdf():
    """服务端 PDF 导出路由（备用方案，长表格自动分页）"""
    EXCEL_FILE = 'fund_open_fund_rank_em.xlsx'

    if not PDF_SERVER_AVAILABLE:
        return jsonify({'code': 1, 'msg': '服务端PDF功能未启用（缺少 reportlab 依赖），请使用前端"导出PDF"按钮'}), 503

    if not os.path.exists(EXCEL_FILE):
        return jsonify({'code': 1, 'msg': 'Excel文件不存在'}), 404

    try:
        df = pd.read_excel(EXCEL_FILE)
        df = df.replace({np.nan: ''})

        # 生成 PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=12 * mm,
            leftMargin=12 * mm,
            topMargin=15 * mm,
            bottomMargin=15 * mm
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontName=PDF_FONT,
            fontSize=18,
            alignment=1,
            spaceAfter=20
        )
        body_style = ParagraphStyle(
            'CustomBody',
            parent=styles['BodyText'],
            fontName=PDF_FONT,
            fontSize=9,
            leading=12
        )

        story = []
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        story.append(Paragraph(f'量化基金排名报告', title_style))
        story.append(Paragraph(f'生成时间: {timestamp}  |  共计 {len(df)} 只基金', body_style))
        story.append(Spacer(1, 6 * mm))

        # 选择要导出的列
        export_columns = [
            '基金代码', '基金简称', '近1月超额', '近3月超额',
            '近6月超额', '近1年超额', '成立时间', '最新规模',
            '换手率', '前10大重仓股占比', '持股行业集中度',
            '管理总规模', '稳定性评分'
        ]
        export_columns = [c for c in export_columns if c in df.columns]

        # 表头
        table_data = [[Paragraph(c, body_style) for c in export_columns]]

        # 数据行（每 40 行强制分页，避免单页过长）
        for i, row in df.iterrows():
            if i > 0 and i % 40 == 0:
                t = Table(table_data, repeatRows=1)
                t.setStyle(TableStyle([
                    ('FONTNAME', (0, 0), (-1, -1), PDF_FONT),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e9fff')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1),
                     [colors.white, colors.HexColor('#f2f2f2')]),
                    ('LEFTPADDING', (0, 0), (-1, -1), 4),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                    ('TOPPADDING', (0, 0), (-1, -1), 4),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ]))
                story.append(t)
                story.append(PageBreak())
                table_data = [[Paragraph(c, body_style) for c in export_columns]]

            row_data = []
            for col in export_columns:
                val = str(row[col]) if row[col] != '' else ''
                row_data.append(Paragraph(val, body_style))
            table_data.append(row_data)

        # 剩余的最后一页表格
        if len(table_data) > 1:
            t = Table(table_data, repeatRows=1)
            t.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, -1), PDF_FONT),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e9fff')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1),
                 [colors.white, colors.HexColor('#f2f2f2')]),
                ('LEFTPADDING', (0, 0), (-1, -1), 4),
                ('RIGHTPADDING', (0, 0), (-1, -1), 4),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ]))
            story.append(t)

        # 页脚页码
        def add_page_number(canvas, doc):
            canvas.saveState()
            canvas.setFont(PDF_FONT if PDF_FONT != 'Helvetica' else 'Helvetica', 9)
            canvas.setFillColor(colors.grey)
            page_num = canvas.getPageNumber()
            text = f'第 {page_num} 页'
            canvas.drawCentredString(A4[0] / 2, 10 * mm, text)
            canvas.restoreState()

        doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
        buffer.seek(0)

        filename = f'量化基金排名_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.pdf'
        encoded_filename = quote(filename)
        return app.response_class(
            response=buffer.getvalue(),
            status=200,
            mimetype='application/pdf',
            headers={
                'Content-Disposition': "attachment; filename=\"fund_ranking.pdf\"; filename*=UTF-8''" + encoded_filename
            }
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'code': 1, 'msg': f'PDF生成失败: {str(e)}'}), 500

@app.route('/export_fund_ranking_pdf')
def export_fund_ranking_pdf():
    """使用 Playwright 将基金排名页面导出为 PDF"""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return jsonify({'code': 1, 'msg': 'Playwright 未安装，请运行: pip install playwright && playwright install chromium'}), 503

    try:
        port = request.environ.get('SERVER_PORT', 5001)
        url = f'http://127.0.0.1:{port}/fund_ranking?print=1'

        # 使用较宽的视口，确保表格完整显示，再由 Playwright 自动缩放至 A4 PDF
        # Layui 表格列宽总和约 1400px，需要视口大于此值
        VP_W, VP_H = 1600, 900

        # 注入到页面最前面的样式（在用户 CSS 之前生效）
        pre_css = '''
            html, body { margin: 0 !important; padding: 0 !important; width: 100% !important; }
            .layui-container { padding: 10px !important; margin: 0 !important; width: 100% !important; }
            .layui-elem-field { border: 1px solid #e6e6e6 !important; margin: 15px 0 !important; padding: 0 !important; }
            .layui-elem-field > legend { display: none !important; }
            .layui-field-box { padding: 10px !important; margin: 0 !important; }
            .chart-container { width: 100% !important; margin: 0 0 15px 0 !important; padding: 0 !important; }
            .echarts-chart { width: 100% !important; height: 450px !important; }
            .echarts-chart canvas { width: 100% !important; height: 450px !important; }
            .table-scroll-container { width: 100% !important; overflow: visible !important; }
            .layui-table-view { width: 100% !important; }
            .layui-table { width: 100% !important; }
        '''

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width': VP_W, 'height': VP_H})

            # 在页面加载前注入样式（拦截 HTML 响应，在 <head> 中 prepend <style>）
            # 这样 ECharts 初始化时就能拿到正确的容器宽度
            def intercept_route(route):
                response = route.fetch()
                body_text = response.text()
                injected = f'<style>{pre_css}</style>'
                body_text = body_text.replace('</head>', injected + '</head>', 1)
                route.fulfill(response=response, body=body_text.encode('utf-8'))

            page.route('**/fund_ranking*', intercept_route)
            page.goto(url, wait_until='networkidle')
            page.wait_for_selector('canvas', timeout=30000)
            page.wait_for_timeout(2000)

            # 强制 ECharts 重新读取容器尺寸并重绘（确保 canvas 像素尺寸匹配容器）
            page.evaluate('''
                () => {
                    if (window.echartsInstances) {
                        window.echartsInstances.forEach(function(inst) {
                            try {
                                var dom = inst.getDom();
                                var w = dom.clientWidth || dom.offsetWidth;
                                var h = dom.clientHeight || 450;
                                inst.resize({width: w, height: h});
                            } catch(e) {}
                        });
                    }
                }
            ''')
            page.wait_for_timeout(2000)

            # 生成 PDF
            pdf_bytes = page.pdf(
                format='A4',
                landscape=True,
                print_background=True,
                margin={'top': '10mm', 'bottom': '10mm', 'left': '10mm', 'right': '10mm'},
                display_header_footer=True,
                header_template='<div style="font-size:10px;width:100%;text-align:center;color:#666;">指数增强超额收益报告</div>',
                footer_template='<div style="font-size:9px;width:100%;text-align:center;color:#999;">第 <span class="pageNumber"></span> / <span class="totalPages"></span> 页</div>'
            )
            browser.close()

        from io import BytesIO
        buffer = BytesIO(pdf_bytes)
        buffer.seek(0)

        import pandas as pd
        filename = f'指数增强超额收益报告_{pd.Timestamp.now().strftime("%Y%m%d")}.pdf'
        encoded_filename = quote(filename)
        return app.response_class(
            response=buffer.getvalue(),
            status=200,
            mimetype='application/pdf',
            headers={
                'Content-Disposition': "attachment; filename=\"fund_ranking.pdf\"; filename*=UTF-8''" + encoded_filename
            }
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'code': 1, 'msg': f'PDF生成失败: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True, threaded=True)
