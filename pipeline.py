"""
Главный пайплайн обработки бюджетных данных.

Этапы:
0. Обработка входных данных (архивы, поиск нужных файлов, извлечение общих сумм)
1. Извлечение всех статей из Excel/Word файлов
2. Классификация через LLM (капитальные/не капитальные)
3. Подсчёт сумм и формирование сводных документов

Использование:
    python pipeline.py                    # Полный пайплайн
    python pipeline.py --skip-llm         # Пропустить LLM (использовать ключевые слова)
    python pipeline.py --from-classified  # Начать с уже классифицированных данных
"""

import os
import sys
import argparse
import json
from datetime import datetime


def run_input_processing():
    """Этап 0: Обработка входных данных (архивы, поиск файлов, извлечение общих сумм)."""
    print("\n" + "=" * 60)
    print("ЭТАП 0: ОБРАБОТКА ВХОДНЫХ ДАННЫХ")
    print("=" * 60)
    
    from input_processor import InputProcessor
    
    processor = InputProcessor()
    found_files, budget_totals = processor.process_all()
    
    # Сохраняем общие суммы бюджетов
    if budget_totals:
        totals_path = "output/budget_totals_from_decisions.json"
        os.makedirs(os.path.dirname(totals_path), exist_ok=True)
        
        totals_data = {}
        for mo, bt in budget_totals.items():
            totals_data[mo] = {
                'year': bt.year,
                'total_income': bt.total_income,
                'total_expenses': bt.total_expenses,
                'source_file': bt.source_file
            }
        
        with open(totals_path, 'w', encoding='utf-8') as f:
            json.dump(totals_data, f, ensure_ascii=False, indent=2)
        print(f"\nОбщие суммы из решений сохранены: {totals_path}")
    
    # Выбираем ОДИН лучший файл бюджета для каждого МО
    budget_files = processor.get_budget_expense_files(one_per_mo=True)
    
    return budget_files, budget_totals


def run_extraction(budget_files=None):
    """Этап 1: Извлечение статей из Excel/Word файлов."""
    print("\n" + "=" * 60)
    print("ЭТАП 1: ИЗВЛЕЧЕНИЕ СТАТЕЙ")
    print("=" * 60)
    
    from extract_articles import extract_all_articles, ArticleExtractor
    
    if budget_files:
        # Используем файлы от input_processor
        print(f"Обработка {len(budget_files)} файлов от input_processor...")
        extractor = ArticleExtractor()
        
        for file_info in budget_files:
            if file_info.format == 'excel':
                extractor.process_excel(
                    filepath=file_info.path,
                    mo_name=file_info.mo_name,
                    target_sheet_name=file_info.sheet_name,
                    target_header_idx=file_info.header_row
                )
            elif file_info.format == 'word':
                # Обработка Word таблиц
                tables_data = file_info.details.get('tables_data')
                extractor.process_word(
                    filepath=file_info.path,
                    mo_name=file_info.mo_name,
                    tables_data=tables_data
                )
    else:
        # Старый режим - сканируем папку input/
        extractor = extract_all_articles()
    
    if extractor.articles:
        output_path = extractor.save_articles_json()
        return output_path, extractor.articles, list(extractor.all_years)
    else:
        print("Статьи не найдены!")
        return None, [], []


def run_llm_classification(articles, years):
    """Этап 2: Классификация через LLM."""
    print("\n" + "=" * 60)
    print("ЭТАП 2: КЛАССИФИКАЦИЯ ЧЕРЕЗ LLM")
    print("=" * 60)
    
    from llm_classifier import LLMClassifier
    
    classifier = LLMClassifier()
    filtered, results = classifier.filter_capital_articles(articles)
    
    # Сохраняем лог классификации
    classifier.save_classification_log(results)
    
    # Сохраняем отфильтрованные статьи
    output_path = "output/classified_articles.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    output_data = {
        'years': years,
        'articles': filtered,
        'classification_stats': classifier.stats,
        'timestamp': datetime.now().isoformat()
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"\nОтфильтрованные статьи сохранены: {output_path}")
    
    return filtered, years


def run_keyword_classification(articles, years):
    """Альтернатива LLM: классификация по ключевым словам."""
    print("\n" + "=" * 60)
    print("ЭТАП 2: КЛАССИФИКАЦИЯ ПО КЛЮЧЕВЫМ СЛОВАМ (без LLM)")
    print("=" * 60)
    
    # Используем логику из extract_data.py
    KEYWORDS_INCLUDE = [
        "строительство", "реконструкция", "капитальный ремонт", "проектирование",
        "проектно-сметн", "создание", "модернизация",
        "обустройство", "озеленение", "благоустройство", 
        "городская среда", "комфортная городская", "рекультивация", "инвестиции",
        "бюджетные инвестиции"
    ]
    
    KEYWORDS_EXCLUDE = [
        "жилой фонд", "жилого фонда", "многоквартирн", "текущий ремонт", "содержание",
        "обслуживание", "приобретение жилья", "приобретение жилых", "взнос", 
        "переселение", "жилого помещения", "жилищное хозяйство"
    ]
    
    def check_keywords(text):
        norm_text = str(text).lower().strip()
        for exc in KEYWORDS_EXCLUDE:
            if exc in norm_text:
                return False, f"Exclude: {exc}"
        for inc in KEYWORDS_INCLUDE:
            if inc in norm_text:
                return True, f"Include: {inc}"
        return False, "No keywords"
    
    filtered = []
    accepted = 0
    rejected = 0
    
    for article in articles:
        name = article.get('name', '')
        is_capital, reason = check_keywords(name)
        
        if is_capital:
            article_copy = article.copy()
            article_copy['llm_reason'] = reason
            filtered.append(article_copy)
            accepted += 1
        else:
            rejected += 1
    
    print(f"  Всего статей: {len(articles)}")
    print(f"  Капитальные (принято): {accepted}")
    print(f"  Отклонено: {rejected}")
    
    # Сохраняем результат
    output_path = "output/classified_articles.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    output_data = {
        'years': years,
        'articles': filtered,
        'classification_stats': {
            'total_processed': len(articles),
            'accepted': accepted,
            'rejected': rejected,
            'method': 'keywords'
        },
        'timestamp': datetime.now().isoformat()
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"\nОтфильтрованные статьи сохранены: {output_path}")
    
    return filtered, years


def run_cost_calculation(articles=None, years=None):
    """Этап 3: Подсчёт стоимости и формирование отчётов."""
    print("\n" + "=" * 60)
    print("ЭТАП 3: ПОДСЧЁТ СТОИМОСТИ И ФОРМИРОВАНИЕ ОТЧЁТОВ")
    print("=" * 60)
    
    from calculate_costs import CostCalculator
    
    calculator = CostCalculator()
    
    if articles is not None:
        # Данные переданы напрямую
        calculator.load_filtered_articles(articles, years)
    else:
        # Загружаем из файла
        json_path = "output/classified_articles.json"
        if not os.path.exists(json_path):
            print(f"Файл {json_path} не найден!")
            return None
        calculator.load_from_json(json_path)
    
    calculator.deduplicate()
    
    # Сохраняем детальные данные
    df = calculator.save_to_excel("output/budget_capital_expenses.xlsx")
    
    # Выводим сводку
    calculator.print_summary()
    
    # Рассчитываем итоги
    totals = calculator.calculate_totals()
    
    # Сохраняем итоги в JSON
    totals_path = "output/budget_totals.json"
    with open(totals_path, 'w', encoding='utf-8') as f:
        json.dump(totals, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nИтоговые суммы сохранены: {totals_path}")
    
    return calculator


def run_summary_report():
    """Создание сводного отчёта (если есть make_summary_report.py)."""
    print("\n" + "=" * 60)
    print("ЭТАП 4: ФОРМИРОВАНИЕ СВОДНОГО ОТЧЁТА")
    print("=" * 60)
    
    try:
        from make_summary_report import create_summary_report
        create_summary_report()
    except ImportError:
        print("Модуль make_summary_report.py не найден, пропускаем.")
    except Exception as e:
        print(f"Ошибка при создании отчёта: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Пайплайн обработки бюджетных данных",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python pipeline.py                    # Полный пайплайн с LLM
  python pipeline.py --skip-llm         # Без LLM, только по ключевым словам
  python pipeline.py --from-classified  # Только подсчёт (данные уже классифицированы)
  python pipeline.py --extract-only     # Только извлечение статей
        """
    )
    
    parser.add_argument(
        '--skip-llm', 
        action='store_true',
        help='Пропустить LLM классификацию, использовать ключевые слова'
    )
    
    parser.add_argument(
        '--from-classified',
        action='store_true', 
        help='Начать с уже классифицированных данных (пропустить этапы 1-2)'
    )
    
    parser.add_argument(
        '--extract-only',
        action='store_true',
        help='Только извлечь статьи (этап 1)'
    )
    
    parser.add_argument(
        '--no-report',
        action='store_true',
        help='Не создавать сводный отчёт'
    )
    
    parser.add_argument(
        '--skip-input-processing',
        action='store_true',
        help='Пропустить обработку входных данных (использовать старый режим сканирования)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("ПАЙПЛАЙН ОБРАБОТКИ БЮДЖЕТНЫХ ДАННЫХ")
    print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    articles = None
    years = None
    budget_files = None
    budget_totals = None
    
    # Этап 0: Обработка входных данных
    if not args.from_classified and not args.skip_input_processing:
        budget_files, budget_totals = run_input_processing()
    
    # Этап 1: Извлечение
    if not args.from_classified:
        output_path, articles, years = run_extraction(budget_files)
        
        if not articles:
            print("\nПайплайн остановлен: нет данных для обработки.")
            return 1
        
        if args.extract_only:
            print("\n" + "=" * 60)
            print("ЗАВЕРШЕНО (только извлечение)")
            print("=" * 60)
            return 0
    
    # Этап 2: Классификация
    if not args.from_classified:
        if args.skip_llm:
            articles, years = run_keyword_classification(articles, years)
        else:
            articles, years = run_llm_classification(articles, years)
        
        if not articles:
            print("\nПайплайн остановлен: все статьи отфильтрованы.")
            return 1
    
    # Этап 3: Подсчёт стоимости
    calculator = run_cost_calculation(articles, years)
    
    if calculator is None:
        print("\nОшибка при подсчёте стоимости.")
        return 1
    
    # Этап 4: Сводный отчёт
    if not args.no_report:
        run_summary_report()
    
    print("\n" + "=" * 60)
    print("ПАЙПЛАЙН ЗАВЕРШЁН УСПЕШНО")
    print("=" * 60)
    print("\nВыходные файлы:")
    print("  output/budget_totals_from_decisions.json - общие суммы из решений")
    print("  output/extracted_articles.json           - все извлечённые статьи")
    print("  output/classified_articles.json          - классифицированные статьи")
    print("  output/llm_classification_log.txt        - лог LLM классификации")
    print("  output/budget_capital_expenses.xlsx      - детальные данные")
    print("  output/budget_totals.json                - итоговые суммы по капитальным расходам")
    
    # Выводим общие суммы из решений
    if budget_totals:
        print("\nОбщие суммы бюджетов (из решений):")
        for mo, bt in budget_totals.items():
            print(f"  {mo}: доходы={bt.total_income:,.2f}, расходы={bt.total_expenses:,.2f}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

