"""
Модуль подсчёта стоимости и формирования итоговых таблиц.

Описание:
    Принимает отфильтрованные статьи (после LLM или по ключевым словам)
    и формирует итоговые таблицы с группировкой по МО и категориям.

Категории расходов:
    - Объекты образования (раздел 07)
    - Объекты здравоохранения, культуры, спорта (разделы 08, 11)
    - Транспортная инфраструктура (разделы 0408, 0409)
    - Благоустройство и ЖКХ (разделы 0502, 0503)

Классификация по коду раздела (РзПр):
    - 07xx → Образование
    - 08xx, 11xx → Здравоохранение, культура, спорт
    - 0408, 0409 → Транспорт
    - 0502, 0503 → ЖКХ
    - Остальное → по ключевым словам или ЖКХ по умолчанию

Основные классы:
    - CostCalculator - калькулятор стоимости

Входные данные:
    - output/classified_articles.json - классифицированные статьи

Выходные данные:
    - output/budget_capital_expenses.xlsx - детализация по статьям
    - output/budget_totals.json - итоговые суммы

Использование:
    python calculate_costs.py [input.json]

    или как модуль:
    from calculate_costs import CostCalculator
    calc = CostCalculator()
    calc.load_from_json("classified_articles.json")
    calc.save_to_excel("output.xlsx")

Автор: Автоматически сгенерировано
Версия: 1.0
"""

import os
import pandas as pd
import json
import warnings

warnings.simplefilter("ignore")

# --- КОНФИГУРАЦИЯ КАТЕГОРИЙ ---
CAT_KEYWORDS = {
    "Объекты образования": [
        "школа", "сош", "лицей", "гимназия", "детский сад", "лагерь", "дошкольное", 
        "автогородок", "образовательная", "молодежь"
    ],
    "Объекты здравоохранения, культуры, спорта": [
        "клуб", "культурный", "досуговый", "физкультур", "фок", "спорт", 
        "бассейн", "стадион", "ледовый", "лыжероллер", "выставочный", 
        "библиотека", "больница", "здравоохранение"
    ],
    "Транспортная инфраструктура": [
        "автомобильная дорога", "дорога", "улично", "тротуар", "мост", 
        "путепровод", "остановочный", "дорожное движение"
    ],
    "Благоустройство и ЖКХ": [
        "водоснабжение", "водопровод", "водовод", "водоотведение", "канализация", 
        "очистные", "теплоснабжение", "тепловая", "котельная", "газификация", 
        "газопровод", "благоустройство", "городская среда", "свалка", "контейнерная", 
        "озеленение", "кладбище", "пожарная", "гидротехнические", "колумбарий", 
        "освещение", "энергосбережение"
    ]
}


def normalize_text(text):
    if pd.isna(text): return ""
    return str(text).lower().strip()


def classify_category(rz_code, text):
    """
    Классифицирует статью по категории на основе кода раздела/подраздела или ключевых слов.
    
    Схема классификации (из инструкции):
    - 07xx (ОБРАЗОВАНИЕ) → "Объекты образования"
    - 08xx (КУЛЬТУРА) → "Объекты здравоохранения, культуры, спорта"
    - 11xx (ФИЗКУЛЬТУРА И СПОРТ) → "Объекты здравоохранения, культуры, спорта"
    - 0408, 0409 (Транспорт, Дорожное хозяйство) → "Транспортная инфраструктура"
    - 0502, 0503 (Коммунальное хозяйство, Благоустройство) → "Благоустройство и ЖКХ"
    - 0501 (Жилищное хозяйство) → НЕ должны сюда попадать (фильтруются на этапе LLM)
    """
    if rz_code:
        code_str = str(rz_code).strip().replace('.', '').replace(' ', '')
        # Нормализуем код до 4 цифр
        if len(code_str) < 4:
            code_str = code_str.zfill(4)
        
        section = code_str[:2]      # Раздел (первые 2 цифры)
        subsection = code_str[:4]   # Подраздел (4 цифры)
        
        # 1. ОБРАЗОВАНИЕ (раздел 07)
        if section == "07":
            return "Объекты образования"
        
        # 2. КУЛЬТУРА (раздел 08) или ФИЗКУЛЬТУРА И СПОРТ (раздел 11)
        if section in ["08", "11"]:
            return "Объекты здравоохранения, культуры, спорта"
        
        # 3. НАЦИОНАЛЬНАЯ ЭКОНОМИКА (раздел 04)
        # Только подразделы Транспорт (0408) и Дорожное хозяйство (0409)
        if section == "04":
            if subsection in ["0408", "0409"]:
                return "Транспортная инфраструктура"
            # Другие подразделы 04 - по ключевым словам ниже
        
        # 4. ЖКХ (раздел 05)
        if section == "05":
            # 0501 = Жилищное хозяйство - обычно НЕ берём, но если прошло LLM - в ЖКХ
            # 0502 = Коммунальное хозяйство → ЖКХ
            # 0503 = Благоустройство → ЖКХ
            if subsection in ["0501", "0502", "0503"]:
                return "Благоустройство и ЖКХ"
    
    # 5. Если раздел не определён - классифицируем по ключевым словам
    norm_text = normalize_text(text)
    for category, keywords in CAT_KEYWORDS.items():
        for kw in keywords:
            if kw in norm_text:
                return category
    
    # 6. По умолчанию - Благоустройство и ЖКХ
    return "Благоустройство и ЖКХ"


def is_generic_name(text):
    """Проверяет, является ли название слишком общим (для дедупликации)."""
    t = normalize_text(text)
    generics = ["бюджетные инвестиции", "капитальные вложения", "субсидии", "иные межбюджетные трансферты"]
    for g in generics:
        if g in t and len(t) < len(g) + 30: 
            return True
    return False


class CostCalculator:
    """
    Принимает отфильтрованные статьи и формирует итоговые таблицы.
    """
    
    def __init__(self):
        self.results = []
        self.all_years = set()
    
    def load_filtered_articles(self, articles, years=None):
        """
        Загружает отфильтрованные статьи (после LLM или ручной фильтрации).
        
        Args:
            articles: список статей в формате:
                [{'file': ..., 'mo': ..., 'rz': ..., 'csr': ..., 'vr': ..., 
                  'name': ..., 'sums': {2025: ..., 2026: ...}, 'category': ... (опционально)}]
            years: список годов (если не указан, определяется автоматически)
        """
        if years:
            self.all_years = set(years)
        
        for article in articles:
            # Определяем годы из данных
            if 'sums' in article:
                self.all_years.update(article['sums'].keys())
            
            # Определяем категорию если не указана
            category = article.get('category')
            if not category:
                category = classify_category(article.get('rz'), article.get('name', ''))
            
            item = {
                "Файл": article.get('file', ''),
                "МО": article.get('mo', ''),
                "Код РзПр": article.get('rz', ''),
                "Код ЦСР": article.get('csr', ''),
                "Код ВР": article.get('vr', ''),
                "Наименование": article.get('name', ''),
                "Категория": category,
                "LLM_причина": article.get('llm_reason', '')  # Причина от LLM классификатора
            }
            
            # Добавляем суммы по годам
            if 'sums' in article:
                for year, value in article['sums'].items():
                    item[int(year)] = value
            
            self.results.append(item)
        
        print(f"Загружено статей: {len(self.results)}")
    
    def load_from_json(self, json_path):
        """Загружает статьи из JSON файла."""
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        years = data.get('years', [])
        articles = data.get('articles', [])
        
        self.load_filtered_articles(articles, years)
    
    def deduplicate(self):
        """Удаляет дубликаты статей с одинаковыми суммами."""
        if not self.results: 
            return
        
        print("\nЗапуск дедупликации...")
        
        df = pd.DataFrame(self.results)
        clean_rows = []
        
        year_cols = sorted(list(self.all_years))
        
        for filename, group in df.groupby('Файл'):
            drop_indices = set()
            
            group['sum_hash'] = group.apply(lambda x: tuple(x.get(y, 0) for y in year_cols), axis=1)
            
            for sum_val, subgroup in group.groupby('sum_hash'):
                if all(v == 0 for v in sum_val): 
                    continue
                
                if len(subgroup) > 1:
                    candidates = []
                    for idx, row in subgroup.iterrows():
                        score = len(str(row['Наименование']))
                        if is_generic_name(row['Наименование']):
                            score -= 1000 
                        candidates.append((score, idx))
                    
                    candidates.sort(key=lambda x: x[0], reverse=True)
                    
                    for score, idx in candidates[1:]:
                        drop_indices.add(idx)

            keep_rows = group[~group.index.isin(drop_indices)]
            clean_rows.append(keep_rows)
            
        if clean_rows:
            self.results = pd.concat(clean_rows).drop(columns=['sum_hash'], errors='ignore').to_dict('records')
            print(f"Дедупликация завершена. Осталось {len(self.results)} строк.")
    
    def calculate_totals(self):
        """Рассчитывает итоговые суммы по МО и категориям."""
        if not self.results:
            return {}
        
        df = pd.DataFrame(self.results)
        year_cols = [c for c in df.columns if isinstance(c, int)]
        
        totals = {
            'by_mo': df.groupby('МО')[year_cols].sum().to_dict(),
            'by_category': df.groupby('Категория')[year_cols].sum().to_dict(),
            'by_mo_category': df.groupby(['МО', 'Категория'])[year_cols].sum().reset_index().to_dict('records'),
            'grand_total': df[year_cols].sum().to_dict()
        }
        
        return totals
    
    def save_to_excel(self, output_path="output/extracted_budget_data.xlsx"):
        """Сохраняет результаты в Excel файл."""
        if not self.results:
            print("Нет данных для сохранения.")
            return
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        df = pd.DataFrame(self.results)
        
        min_year = 2024
        max_year = max(self.all_years) if self.all_years else 2026
        
        all_year_cols = list(range(min_year, max_year + 1))
        for y in all_year_cols:
            if y not in df.columns:
                df[y] = 0.0
            else:
                df[y] = df[y].fillna(0.0)

        fixed_cols = ['Файл', 'МО', 'Код РзПр', 'Код ЦСР', 'Код ВР', 'Наименование', 'Категория', 'LLM_причина']
        fixed_cols = [c for c in fixed_cols if c in df.columns]
        
        final_cols = fixed_cols + all_year_cols
        final_cols = [c for c in final_cols if c in df.columns]
        
        df_out = df[final_cols]
        
        df_out.to_excel(output_path, index=False)
        print(f"\nСохранено: {output_path} ({len(df_out)} строк)")
        
        return df_out
    
    def print_summary(self, target_year=2025):
        """Выводит краткую сводку по данным."""
        if not self.results:
            print("Нет данных.")
            return
        
        df = pd.DataFrame(self.results)
        
        if target_year in df.columns:
            summary = df.groupby(['МО', 'Категория'])[target_year].sum().reset_index()
            pd.options.display.float_format = '{:,.2f}'.format
            print(f"\nСводка по {target_year} году:")
            print(summary)
            
            print(f"\nИТОГО по {target_year}: {df[target_year].sum():,.2f}")


def process_filtered_articles(articles, years=None, output_path="output/extracted_budget_data.xlsx"):
    """
    Основная функция обработки отфильтрованных статей.
    
    Args:
        articles: список отфильтрованных статей
        years: список годов
        output_path: путь для сохранения Excel
    
    Returns:
        CostCalculator instance
    """
    calculator = CostCalculator()
    calculator.load_filtered_articles(articles, years)
    calculator.deduplicate()
    calculator.save_to_excel(output_path)
    calculator.print_summary()
    
    return calculator


if __name__ == "__main__":
    import sys
    
    # По умолчанию загружаем результат LLM классификации
    # Можно передать путь к файлу через аргумент командной строки
    default_path = "output/classified_articles.json"
    fallback_path = "output/extracted_articles.json"
    
    json_path = sys.argv[1] if len(sys.argv) > 1 else default_path
    
    # Если файл после LLM не найден, пробуем файл до LLM (для тестирования)
    if not os.path.exists(json_path) and json_path == default_path:
        if os.path.exists(fallback_path):
            print(f"Файл {json_path} не найден, использую {fallback_path}")
            json_path = fallback_path
    
    if os.path.exists(json_path):
        print(f"Загрузка статей из {json_path}...")
        calculator = CostCalculator()
        calculator.load_from_json(json_path)
        calculator.deduplicate()
        calculator.save_to_excel()
        calculator.print_summary()
    else:
        print(f"Файл {json_path} не найден.")
        print("Запустите пайплайн:")
        print("  1. python extract_articles.py")
        print("  2. python llm_classifier.py")
        print("  3. python calculate_costs.py")

