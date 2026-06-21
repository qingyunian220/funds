
from flask import Flask, render_template, jsonify, request
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
    return render_template('quant_fund_ranking.html')

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
                                    return jsonify({'code': 0, 'data': excess_curve})
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
