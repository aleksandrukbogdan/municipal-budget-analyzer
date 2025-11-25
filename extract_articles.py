"""
Модуль извлечения статей из бюджетных файлов Excel.

Описание:
    Извлекает все статьи (Article) из Excel-файлов бюджетов МО
    без фильтрации по ключевым словам. Подготавливает данные
    для последующей классификации через LLM или по ключевым словам.

Основные функции:
    - extract_all_articles() - извлекает статьи из всех файлов в папке input/
    - ArticleExtractor - класс для извлечения статей из Excel

Входные данные:
    - Excel файлы (.xls, .xlsx) из папки input/
    - Файлы должны содержать "Ведомственную структуру расходов бюджета"

Выходные данные:
    - output/extracted_articles.json - JSON со всеми извлечёнными статьями

Использование:
    python extract_articles.py

    или как модуль:
    from extract_articles import extract_all_articles
    extractor = extract_all_articles()
    articles = extractor.get_articles()

Автор: Автоматически сгенерировано
Версия: 1.0
"""

import os
import pandas as pd
import re
import warnings
import json

warnings.simplefilter("ignore")

INPUT_DIR = "input"

def normalize_text(text):
    if pd.isna(text): return ""
    return str(text).lower().strip()

def normalize_code(val):
    if pd.isna(val): return ""
    s = str(val).replace(' ', '').replace('-', '').strip()
    if s in ['0', '0.0', '000', 'nan', '']: return ""
    return s

def get_row_type(rz_val, csr_val, vr_val, name_val):
    """
    Определяет тип строки на основе инструкций.
    Возвращает: 'Section', 'Subsection', 'Program', 'Article', 'ViewExpense'
    """
    rz = normalize_code(rz_val)
    csr = normalize_code(csr_val)
    vr = normalize_code(vr_val)
    name = str(name_val) if pd.notna(name_val) else ""
    
    # 1. Если есть ВР - это Вид расхода или Подвид (ДЕТАЛИЗАЦИЯ - НЕ БЕРЕМ)
    if vr:
        return "ViewExpense"

    # 2. Если нет ЦСР - это Раздел или Подраздел (ЗАГОЛОВОК - НЕ БЕРЕМ)
    if not csr:
        if rz and rz.endswith('00'): return "Section"
        return "Subsection"

    # 3. Если есть ЦСР, но нет ВР - это Программа, Подпрограмма или СТАТЬЯ
    if csr.endswith('00000'): 
         return "Program"

    name_lower = name.lower()
    if "программа" in name_lower:
        return "Program"
        
    return "Article"

def get_sheet_name_fuzzy(xls, target_name):
    if not target_name: return None
    normalized_target = normalize_text(target_name).replace(" ", "")
    for sheet in xls.sheet_names:
        normalized_sheet = normalize_text(sheet).replace(" ", "")
        if normalized_target in normalized_sheet or normalized_sheet in normalized_target:
            return sheet
    return None

def is_valid_budget_file(filename):
    fname_lower = filename.lower()
    
    if "доход" in fname_lower: return False
    if "источник" in fname_lower: return False
    if "дефицит" in fname_lower: return False
    
    if "расход" in fname_lower: return True
    if "ведомств" in fname_lower: return True
    if "прил" in fname_lower and "4" in fname_lower: return True
    if "прил" in fname_lower and "5" in fname_lower: return True
    if "бюджет" in fname_lower and "20" in fname_lower: return True
    
    return False


class ArticleExtractor:
    """
    Извлекает статьи из бюджетных файлов Excel.
    Статьи извлекаются БЕЗ фильтрации по ключевым словам - 
    это будет делать LLM на следующем этапе.
    """
    
    def __init__(self):
        self.articles = []  # Список извлеченных статей
        self.all_years = set()

    def identify_columns(self, df, header_row_idx):
        header = df.iloc[header_row_idx]
        cols = {
            'name': None, 'rz': None, 'pr': None, 'csr': None, 'vr': None, 
            'year_map': {} 
        }
        
        def scan_row_for_cols(row_items, is_header_row=True):
             for idx, val in enumerate(row_items):
                txt = str(val).lower().replace('\n', '').replace('-', '').replace(' ', '').strip()
                
                years = re.findall(r'20[2-3][0-9]', txt)
                if years:
                    y = int(years[0])
                    if y >= 2024:
                        cols['year_map'][y] = idx
                        self.all_years.add(y)

                if is_header_row:
                    if 'наименование' in txt: cols['name'] = idx
                    elif 'раздел' in txt and 'подраздел' not in txt: cols['rz'] = idx
                    elif 'подраздел' in txt: cols['pr'] = idx
                    elif 'раздел' in txt and 'подраздел' in txt: cols['rz'] = idx 
                    elif 'целев' in txt or 'цср' in txt: cols['csr'] = idx
                    elif 'вид' in txt and 'расх' in txt: cols['vr'] = idx
        
        scan_row_for_cols(header, is_header_row=True)
        
        if not cols['year_map'] and header_row_idx + 1 < len(df):
            next_row = df.iloc[header_row_idx + 1]
            scan_row_for_cols(next_row, is_header_row=False)

        if not cols['year_map']:
             sum_start = None
             for idx, val in enumerate(header):
                 txt = str(val).lower()
                 if 'сумма' in txt or 'план' in txt:
                     sum_start = idx
                     break
             
             if sum_start is not None:
                 cols['year_map'][2024] = sum_start
                 cols['year_map'][2025] = sum_start + 1
                 cols['year_map'][2026] = sum_start + 2
                 self.all_years.update([2024, 2025, 2026])

        if cols['name'] is not None:
            n = cols['name']
            if cols['vr'] is None and n > 0: cols['vr'] = n - 1 
            if cols['csr'] is None and n > 1: cols['csr'] = n - 2
            if cols['rz'] is None and n > 3: cols['rz'] = n - 3

        return cols

    def process_excel(self, filepath, mo_name, target_sheet_name=None, target_header_idx=None):
        """
        Обрабатывает Excel файл и извлекает все статьи.
        """
        filename = os.path.basename(filepath)
        print(f"Обработка: {filename} (МО: {mo_name})")
        
        try:
            xls = pd.ExcelFile(filepath)
            sheet_to_use = None
            
            if target_sheet_name and target_sheet_name in xls.sheet_names:
                sheet_to_use = target_sheet_name
            
            if not sheet_to_use:
                sheet_to_use = get_sheet_name_fuzzy(xls, target_sheet_name)
                
            if not sheet_to_use:
                for s in xls.sheet_names:
                    s_lower = s.lower()
                    if ("прил" in s_lower and "4" in s_lower) or ("ведомств" in s_lower) or ("расход" in s_lower):
                        sheet_to_use = s
                        break
            
            if not sheet_to_use:
                 if target_sheet_name == "Прил1": 
                     sheet_to_use = xls.sheet_names[0]

            if not sheet_to_use:
                sheet_to_use = xls.sheet_names[0]
                print(f"  Не найден специфичный лист. Пробуем первый: {sheet_to_use}")

            print(f"  Используем лист: {sheet_to_use}")
            df = pd.read_excel(filepath, sheet_name=sheet_to_use, header=None)
            
            header_idx = target_header_idx
            if header_idx is None or header_idx >= len(df):
                 for idx, row in df.head(40).iterrows():
                     txt = " ".join([str(x).lower() for x in row if pd.notna(x)])
                     if "наименование" in txt and ("раздел" in txt or "код" in txt or "целевая" in txt):
                         header_idx = idx
                         break
            
            if header_idx is None:
                print(f"  Заголовок не найден.")
                return

            col_map = self.identify_columns(df, header_idx)
            
            if col_map['name'] is None or not col_map['year_map']:
                print(f"  Не удалось определить колонки (Имя или Года).")
                return

            years = sorted(col_map['year_map'].keys())
            print(f"  Колонки: Name={col_map['name']}, CSR={col_map['csr']}, Years={years}")

            start_row = header_idx + 1
            if start_row < len(df):
                 check_row = df.iloc[start_row]
                 check_txt = " ".join([str(x).lower() for x in check_row if pd.notna(x)])
                 if "год" in check_txt or any(str(y) in check_txt for y in col_map['year_map'].keys()):
                     start_row += 1

            for idx, row in df.iloc[start_row:].iterrows():
                name = row[col_map['name']]
                if pd.isna(name): continue
                
                rz = row[col_map['rz']] if col_map['rz'] is not None else None
                csr = row[col_map['csr']] if col_map['csr'] is not None else None
                vr = row[col_map['vr']] if col_map['vr'] is not None else None
                
                row_type = get_row_type(rz, csr, vr, name)
                
                # Берем ТОЛЬКО статьи (без фильтрации по ключевым словам!)
                if row_type != "Article":
                    continue

                def clean_sum(val):
                    if pd.isna(val): return 0.0
                    s = str(val).replace(u'\xa0', '').replace(' ', '').replace(',', '.')
                    try: return float(s)
                    except: return 0.0

                row_sums = {}
                has_nonzero = False
                for year, col_idx in col_map['year_map'].items():
                    if col_idx < len(row):
                        val = clean_sum(row[col_idx])
                        if val != 0:
                            has_nonzero = True
                        row_sums[year] = val
                    else:
                        row_sums[year] = 0.0

                # Пропускаем нулевые строки
                if not has_nonzero:
                    continue

                article = {
                    "file": filename,
                    "mo": mo_name,
                    "row_idx": idx,
                    "rz": str(rz) if rz else "",
                    "csr": str(csr) if csr else "",
                    "vr": str(vr) if vr else "",
                    "name": str(name).strip(),
                    "sums": row_sums
                }
                
                self.articles.append(article)

        except Exception as e:
            print(f"Ошибка обработки {filename}: {e}")

    def get_articles(self):
        """Возвращает список извлеченных статей."""
        return self.articles
    
    def get_articles_for_llm(self):
        """
        Возвращает статьи в формате, удобном для передачи в LLM.
        Группирует по файлам для более эффективной обработки.
        """
        by_file = {}
        for article in self.articles:
            fname = article['file']
            if fname not in by_file:
                by_file[fname] = {
                    'mo': article['mo'],
                    'articles': []
                }
            by_file[fname]['articles'].append({
                'idx': article['row_idx'],
                'name': article['name'],
                'rz': article['rz'],
                'csr': article['csr'],
                'sums': article['sums']
            })
        return by_file
    
    def save_articles_json(self, output_path="output/extracted_articles.json"):
        """Сохраняет статьи в JSON для последующей обработки."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        data = {
            'years': sorted(list(self.all_years)),
            'articles': self.articles
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"Статьи сохранены: {output_path} ({len(self.articles)} статей)")
        return output_path


def extract_all_articles(input_dir=INPUT_DIR):
    """
    Основная функция извлечения статей.
    Возвращает экземпляр ArticleExtractor с извлеченными статьями.
    """
    extractor = ArticleExtractor()
    
    print(f"Сканирование папки {input_dir}...")
    if not os.path.exists(input_dir):
        print("Папка input не найдена.")
        return extractor

    files_to_process = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            full_path = os.path.join(root, file)
            
            folder_name = os.path.basename(root)
            if folder_name == 'input':
                mo_name = file.split('_')[0]
            else:
                mo_name = folder_name.split('_')[0]

            if file.lower().endswith(('.xls', '.xlsx')) and not file.startswith('~$'):
                if is_valid_budget_file(file):
                    files_to_process.append((full_path, mo_name))
                else:
                    print(f"Пропущен файл (не бюджет расходов): {file}")

    print(f"\nНайдено корректных Excel-файлов для обработки: {len(files_to_process)}")

    for filepath, mo_name in files_to_process:
        extractor.process_excel(filepath, mo_name)

    print(f"\nВсего извлечено статей: {len(extractor.articles)}")
    return extractor


if __name__ == "__main__":
    # При запуске напрямую - извлекаем все статьи и сохраняем в JSON
    extractor = extract_all_articles()
    
    if extractor.articles:
        extractor.save_articles_json()
        
        # Выводим статистику
        print(f"\nСтатистика по МО:")
        mo_counts = {}
        for art in extractor.articles:
            mo = art['mo']
            mo_counts[mo] = mo_counts.get(mo, 0) + 1
        
        for mo, count in sorted(mo_counts.items()):
            print(f"  {mo}: {count} статей")

