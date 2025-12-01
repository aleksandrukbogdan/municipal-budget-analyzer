"""
Модуль классификации статей бюджета через LLM (Qwen).

Описание:
    Классифицирует статьи бюджета на капитальные и не капитальные расходы
    с помощью LLM модели. Использует vLLM с OpenAI-compatible API.
    Поддерживает батчевую обработку для эффективности.

Критерии капитальных расходов:
    ВКЛЮЧИТЬ: строительство, реконструкция, капремонт, модернизация,
              проектирование, создание объектов, благоустройство
    ИСКЛЮЧИТЬ: содержание, обслуживание, текущий ремонт, жилой фонд,
               выплаты, пособия, организационные мероприятия

Конфигурация (.env):
    QWEN_BASE_URL - адрес API сервера (по умолчанию http://localhost:8880/v1)
    QWEN_API_KEY - API ключ (для локального сервера любой)
    QWEN_MODEL_NAME - название модели (по умолчанию "default")

Основные классы:
    - LLMClassifier - классификатор статей
    - ClassificationResult - результат классификации одной статьи

Выходные данные:
    - output/classified_articles.json - отфильтрованные капитальные статьи
    - output/llm_classification_log.txt - лог классификации

Использование:
    python llm_classifier.py [input.json]

    или как модуль:
    from llm_classifier import LLMClassifier
    classifier = LLMClassifier(batch_size=25)
    filtered, results = classifier.filter_capital_articles(articles)

Автор: Автоматически сгенерировано
Версия: 1.0
"""

import os
import json
import time
import hashlib
import requests
from typing import List, Dict, Optional
from dataclasses import dataclass
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Загружаем .env
load_dotenv()

# --- КОНФИГУРАЦИЯ ---

def _clean_env_value(value: str) -> str:
    """Убирает лишние кавычки из значения переменной окружения."""
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        value = value[1:-1]
    return value.strip()

QWEN_API_KEY = _clean_env_value(os.getenv("QWEN_API_KEY", "sk-dummy-key"))
QWEN_BASE_URL = _clean_env_value(os.getenv("QWEN_BASE_URL", "http://localhost:8880/v1"))
QWEN_MODEL_NAME = _clean_env_value(os.getenv("QWEN_MODEL_NAME", "default"))

# --- СИСТЕМНЫЙ ПРОМПТ ---

SYSTEM_PROMPT = """Ты эксперт по анализу бюджетных документов муниципальных образований России.

Твоя задача: определить, относится ли статья расходов к КАПИТАЛЬНЫМ ЗАТРАТАМ.

=== КАПИТАЛЬНЫЕ ЗАТРАТЫ включают (ОСНОВНЫЕ ключевые слова): ===
- Строительство объектов (нежилых зданий и сооружений)
- Реконструкция объектов
- Капитальный ремонт зданий и сооружений (НЕ жилого фонда!)
- Проектирование, разработка проектно-сметной документации
- Создание новых объектов (ледовый корт, спортплощадка, котельная и т.д.)
- Модернизация объектов
- Обустройство территорий
- Озеленение
- Благоустройство (крупные проекты, НЕ текущее содержание)
- Комплексное благоустройство
- Формирование современной городской среды
- Создание комфортной городской среды
- Рекультивация (свалки, полигоны)
- Бюджетные инвестиции в объекты капитального строительства

=== ОБЪЕКТЫ, которые обычно относятся к капитальным затратам: ===
Образование: школа, детский сад, лагерь, автогородок
Культура и спорт: клуб, культурный центр, досуговый центр, ФОК, спортивный комплекс, бассейн, стадион, спортивная площадка, ледовый корт, лыжероллерная трасса, выставочный зал, библиотека
Транспорт: автомобильная дорога, улично-дорожная сеть, тротуар, мост, путепровод, остановочный комплекс
ЖКХ и инфраструктура: водоснабжение, водопровод, водоотведение, канализация, очистные сооружения, теплоснабжение, тепловая сеть, котельная, газификация, газопровод, наружное освещение, уличное освещение, ливневая канализация, кладбище, пожарная безопасность, гидротехнические сооружения (ГТС), колумбарий, контейнерные площадки

=== НЕ ЯВЛЯЮТСЯ капитальными затратами (ИСКЛЮЧИТЬ): ===
- Жилой фонд, многоквартирный дом (МКД)
- Текущий ремонт
- Текущее содержание
- Содержание (эксплуатационные расходы)
- Обслуживание
- Приобретение/покупка жилья, жилых помещений
- Взносы в фонд капитального ремонта (МКД)
- Переселение, расселение из аварийного жилья
- Выплаты, пособия, компенсации гражданам
- Организационные мероприятия без создания объектов
- Финансовое обеспечение текущей деятельности
- Субсидии на оплату ЖКУ
- Пополнение книжного фонда библиотек

ВАЖНО: Если статья содержит слово "строительство", "реконструкция", "капитальный ремонт", "модернизация", "создание", "благоустройство" применительно к НЕжилым объектам - это КАПИТАЛЬНЫЕ затраты.

Отвечай СТРОГО в формате JSON."""


BATCH_PROMPT_TEMPLATE = """Проанализируй следующие статьи бюджета и определи, какие из них относятся к КАПИТАЛЬНЫМ ЗАТРАТАМ.

Статьи для анализа:
{articles_text}

Верни JSON-массив с результатами для КАЖДОЙ статьи:
```json
[
  {{"idx": 0, "is_capital": true, "reason": "строительство"}},
  {{"idx": 1, "is_capital": false, "reason": "жилой фонд"}},
  ...
]
```

ВАЖНО:
- idx - порядковый номер статьи (начиная с 0)
- is_capital - true если капитальные затраты, false если нет
- reason - краткое обоснование (1-2 слова: "строительство", "реконструкция", "содержание", "жилой фонд" и т.д.)
- Верни результат для ВСЕХ {count} статей"""


@dataclass
class ClassificationResult:
    """Результат классификации одной статьи."""
    article_idx: int
    article_name: str
    is_capital: bool
    reason: str
    category: Optional[str] = None  # Категория капитальных затрат
    raw_response: Optional[str] = None


class LLMClassifier:
    """
    Классификатор статей бюджета через LLM.
    Проверяет принадлежность к капитальным расходам.
    """
    
    CACHE_FILE = "output/.llm_cache.json"
    
    def __init__(
        self,
        base_url: str = QWEN_BASE_URL,
        api_key: str = QWEN_API_KEY,
        model_name: str = QWEN_MODEL_NAME,
        batch_size: int = 20,  # Уменьшен с 25 для стабильности
        timeout: int = 180,    # Увеличен с 120 для больших батчей
        max_retries: int = 3,
        retry_delay: float = 2.0,
        max_parallel_requests: int = 3,  # Уменьшено с 4 для снижения нагрузки на LLM
        use_cache: bool = True  # Использовать кэш результатов
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.model_name = model_name
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.max_parallel_requests = max_parallel_requests
        self.use_cache = use_cache
        
        # Кэш результатов классификации
        self._cache = self._load_cache() if use_cache else {}
        self._cache_lock = threading.Lock()
        
        # Статистика (потокобезопасная)
        self._stats_lock = threading.Lock()
        self.stats = {
            'total_processed': 0,
            'accepted': 0,
            'rejected': 0,
            'errors': 0,
            'api_calls': 0,
            'cache_hits': 0
        }
    
    def _load_cache(self) -> Dict:
        """Загружает кэш из файла."""
        if os.path.exists(self.CACHE_FILE):
            try:
                with open(self.CACHE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def _save_cache(self):
        """Сохраняет кэш в файл."""
        os.makedirs(os.path.dirname(self.CACHE_FILE), exist_ok=True)
        with self._cache_lock:
            with open(self.CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False)
    
    def _get_cache_key(self, article_name: str) -> str:
        """Генерирует ключ кэша для статьи."""
        normalized = article_name.lower().strip()
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def _get_from_cache(self, article_name: str) -> Optional[Dict]:
        """Получает результат из кэша."""
        if not self.use_cache:
            return None
        key = self._get_cache_key(article_name)
        with self._cache_lock:
            return self._cache.get(key)
    
    def _add_to_cache(self, article_name: str, is_capital: bool, reason: str, category: Optional[str] = None):
        """Добавляет результат в кэш."""
        if not self.use_cache:
            return
        key = self._get_cache_key(article_name)
        with self._cache_lock:
            self._cache[key] = {'is_capital': is_capital, 'reason': reason, 'category': category}
    
    def _update_stats(self, **kwargs):
        """Потокобезопасное обновление статистики."""
        with self._stats_lock:
            for key, value in kwargs.items():
                if key in self.stats:
                    self.stats[key] += value
    
    def _make_request(self, messages: List[Dict], temperature: float = 0.1) -> Optional[str]:
        """
        Выполняет запрос к API LLM.
        
        Args:
            messages: Список сообщений для chat completion
            temperature: Температура генерации (низкая для детерминированности)
        
        Returns:
            Текст ответа или None при ошибке
        """
        url = f"{self.base_url}/chat/completions"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        payload = {
            "model": self.model_name,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 4096
        }
        
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout
                )
                
                self._update_stats(api_calls=1)
                
                if response.status_code == 200:
                    data = response.json()
                    return data['choices'][0]['message']['content']
                else:
                    print(f"  API Error {response.status_code}: {response.text[:200]}")
                    
            except requests.exceptions.Timeout:
                print(f"  Timeout (attempt {attempt + 1}/{self.max_retries})")
            except requests.exceptions.RequestException as e:
                print(f"  Request error: {e}")
            except Exception as e:
                print(f"  Unexpected error: {e}")
            
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay * (attempt + 1))
        
        return None
    
    def _parse_json_response(self, response_text: str) -> Optional[List[Dict]]:
        """
        Парсит JSON из ответа LLM.
        Обрабатывает случаи когда JSON обёрнут в markdown code block.
        """
        if not response_text:
            return None
        
        # Убираем markdown code blocks
        text = response_text.strip()
        if text.startswith("```json"):
            text = text[7:]
        elif text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Попробуем найти JSON массив в тексте
            start = text.find('[')
            end = text.rfind(']') + 1
            if start != -1 and end > start:
                try:
                    return json.loads(text[start:end])
                except json.JSONDecodeError:
                    pass
        
        return None
    
    def classify_batch(self, articles: List[Dict]) -> List[ClassificationResult]:
        """
        Классифицирует батч статей.
        
        Args:
            articles: Список статей [{'idx': ..., 'name': ...}, ...]
        
        Returns:
            Список ClassificationResult
        """
        if not articles:
            return []
        
        # Формируем текст статей для промпта
        articles_lines = []
        for i, art in enumerate(articles):
            name = art.get('name', '').strip()
            articles_lines.append(f"{i}. {name}")
        
        articles_text = "\n".join(articles_lines)
        
        user_prompt = BATCH_PROMPT_TEMPLATE.format(
            articles_text=articles_text,
            count=len(articles)
        )
        
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ]
        
        response_text = self._make_request(messages)
        
        results = []
        parsed = self._parse_json_response(response_text)
        
        if parsed and isinstance(parsed, list):
            # Создаём словарь результатов по idx
            result_map = {}
            for item in parsed:
                if isinstance(item, dict) and 'idx' in item:
                    result_map[item['idx']] = item
            
            # Формируем результаты для всех статей
            batch_processed = 0
            batch_accepted = 0
            batch_rejected = 0
            batch_errors = 0
            
            for i, art in enumerate(articles):
                if i in result_map:
                    item = result_map[i]
                    is_capital = item.get('is_capital', False)
                    reason = item.get('reason', '')
                    category = item.get('category') if is_capital else None
                else:
                    # Статья не найдена в ответе - помечаем как ошибку
                    is_capital = False
                    reason = "LLM_NO_RESPONSE"
                    category = None
                    batch_errors += 1
                
                result = ClassificationResult(
                    article_idx=art.get('original_idx', i),
                    article_name=art.get('name', ''),
                    is_capital=is_capital,
                    reason=reason,
                    category=category,
                    raw_response=response_text if i == 0 else None
                )
                results.append(result)
                
                batch_processed += 1
                if is_capital:
                    batch_accepted += 1
                else:
                    batch_rejected += 1
            
            self._update_stats(
                total_processed=batch_processed,
                accepted=batch_accepted,
                rejected=batch_rejected,
                errors=batch_errors
            )
        else:
            # Ошибка парсинга - помечаем все статьи
            for art in articles:
                result = ClassificationResult(
                    article_idx=art.get('original_idx', 0),
                    article_name=art.get('name', ''),
                    is_capital=False,
                    reason="LLM_PARSE_ERROR",
                    raw_response=response_text
                )
                results.append(result)
            
            self._update_stats(
                errors=len(articles),
                total_processed=len(articles),
                rejected=len(articles)
            )
        
        return results
    
    def classify_articles(
        self, 
        articles: List[Dict],
        progress_callback: Optional[callable] = None
    ) -> List[ClassificationResult]:
        """
        Классифицирует все статьи с разбивкой на батчи.
        Использует параллельную обработку для ускорения.
        
        Args:
            articles: Список статей [{'name': ..., 'sums': {...}, ...}, ...]
            progress_callback: Функция обратного вызова для прогресса
        
        Returns:
            Список ClassificationResult
        """
        total = len(articles)
        
        # Подготавливаем статьи с индексами
        indexed_articles = [
            {'original_idx': i, 'name': art.get('name', '')}
            for i, art in enumerate(articles)
        ]
        
        # Разбиваем на батчи
        batches = []
        for batch_start in range(0, total, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total)
            batch = indexed_articles[batch_start:batch_end]
            batches.append((batch_start, batch_end, batch))
        
        print(f"  Разбито на {len(batches)} батчей, параллельных запросов: {self.max_parallel_requests}")
        
        # Словарь для хранения результатов с сохранением порядка
        results_by_batch = {}
        processed_count = 0
        processed_lock = threading.Lock()
        
        def process_batch(batch_info):
            nonlocal processed_count
            batch_start, batch_end, batch = batch_info
            batch_results = self.classify_batch(batch)
            
            with processed_lock:
                processed_count += len(batch)
                print(f"  Обработано {processed_count}/{total} статей...")
                if progress_callback:
                    progress_callback(processed_count, total)
            
            return batch_start, batch_results
        
        # Параллельная обработка батчей
        with ThreadPoolExecutor(max_workers=self.max_parallel_requests) as executor:
            futures = {executor.submit(process_batch, batch_info): batch_info 
                       for batch_info in batches}
            
            for future in as_completed(futures):
                try:
                    batch_start, batch_results = future.result()
                    results_by_batch[batch_start] = batch_results
                except Exception as e:
                    batch_info = futures[future]
                    print(f"  Ошибка в батче {batch_info[0]}-{batch_info[1]}: {e}")
                    # Помечаем все статьи батча как ошибочные
                    batch_start, batch_end, batch = batch_info
                    error_results = [
                        ClassificationResult(
                            article_idx=art.get('original_idx', 0),
                            article_name=art.get('name', ''),
                            is_capital=False,
                            reason="THREAD_ERROR"
                        ) for art in batch
                    ]
                    results_by_batch[batch_start] = error_results
                    self._update_stats(
                        errors=len(batch),
                        total_processed=len(batch),
                        rejected=len(batch)
                    )
        
        # Собираем результаты в правильном порядке
        all_results = []
        for batch_start in sorted(results_by_batch.keys()):
            all_results.extend(results_by_batch[batch_start])
        
        return all_results
    
    def filter_capital_articles(
        self, 
        articles: List[Dict]
    ) -> tuple[List[Dict], List[ClassificationResult]]:
        """
        Фильтрует статьи, оставляя только капитальные расходы.
        Использует кэш для пропуска уже классифицированных статей.
        
        Args:
            articles: Список статей из extract_articles.py
        
        Returns:
            Tuple (отфильтрованные статьи, все результаты классификации)
        """
        print(f"\nКлассификация {len(articles)} статей через LLM...")
        print(f"  Модель: {self.model_name}")
        print(f"  Размер батча: {self.batch_size}")
        print(f"  Параллельных запросов: {self.max_parallel_requests}")
        print(f"  Кэш: {'включён' if self.use_cache else 'отключён'}")
        
        # Разделяем статьи на кэшированные и новые
        cached_results = []
        articles_to_process = []
        articles_to_process_indices = []
        
        for i, art in enumerate(articles):
            name = art.get('name', '')
            cached = self._get_from_cache(name)
            if cached:
                # Результат есть в кэше
                result = ClassificationResult(
                    article_idx=i,
                    article_name=name,
                    is_capital=cached['is_capital'],
                    reason=cached['reason'] + " (cached)",
                    category=cached.get('category')
                )
                cached_results.append(result)
                self._update_stats(
                    total_processed=1,
                    cache_hits=1,
                    accepted=1 if cached['is_capital'] else 0,
                    rejected=0 if cached['is_capital'] else 1
                )
            else:
                articles_to_process.append(art)
                articles_to_process_indices.append(i)
        
        if cached_results:
            print(f"  Из кэша: {len(cached_results)} статей")
        
        # Классифицируем только новые статьи
        new_results = []
        if articles_to_process:
            print(f"  Новых для классификации: {len(articles_to_process)}")
            new_results = self.classify_articles(articles_to_process)
            
            # Корректируем индексы и добавляем в кэш
            for j, result in enumerate(new_results):
                original_idx = articles_to_process_indices[j]
                result.article_idx = original_idx
                # Добавляем в кэш (с категорией)
                self._add_to_cache(result.article_name, result.is_capital, result.reason, result.category)
        
        # Сохраняем кэш
        if self.use_cache and new_results:
            self._save_cache()
            print(f"  Кэш обновлён: {len(self._cache)} записей")
        
        # Объединяем результаты
        all_results = cached_results + new_results
        all_results.sort(key=lambda r: r.article_idx)
        
        # Фильтруем капитальные
        filtered = []
        for result in all_results:
            if result.is_capital:
                # Копируем оригинальную статью и добавляем результат классификации
                article = articles[result.article_idx].copy()
                article['llm_reason'] = result.reason
                article['llm_category'] = result.category  # Категория от LLM
                filtered.append(article)
        
        print(f"\nСтатистика классификации:")
        print(f"  Всего обработано: {self.stats['total_processed']}")
        print(f"  Из кэша: {self.stats['cache_hits']}")
        print(f"  Капитальные (принято): {self.stats['accepted']}")
        print(f"  Не капитальные (отклонено): {self.stats['rejected']}")
        print(f"  Ошибок: {self.stats['errors']}")
        print(f"  API вызовов: {self.stats['api_calls']}")
        
        return filtered, all_results
    
    def save_classification_log(
        self, 
        results: List[ClassificationResult],
        output_path: str = "output/llm_classification_log.txt"
    ):
        """Сохраняет детальный лог классификации."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("LLM CLASSIFICATION LOG\n")
            f.write("=" * 140 + "\n")
            f.write(f"Model: {self.model_name}\n")
            f.write(f"Total: {len(results)}\n\n")
            
            f.write(f"{'IDX':<6} | {'STATUS':<8} | {'CATEGORY':<40} | {'REASON':<20} | {'NAME':<50}\n")
            f.write("-" * 140 + "\n")
            
            for r in results:
                status = "CAPITAL" if r.is_capital else "SKIP"
                category = (r.category or "")[:40]
                name = r.article_name[:50]
                reason = (r.reason or "")[:20]
                f.write(f"{r.article_idx:<6} | {status:<8} | {category:<40} | {reason:<20} | {name}\n")
            
            f.write("-" * 140 + "\n")
            f.write(f"\nСтатистика:\n")
            f.write(f"  Капитальные: {self.stats['accepted']}\n")
            f.write(f"  Отклонено: {self.stats['rejected']}\n")
            f.write(f"  Ошибок: {self.stats['errors']}\n")
        
        print(f"Лог классификации сохранён: {output_path}")


def classify_from_json(
    input_json: str = "output/extracted_articles.json",
    output_json: str = "output/classified_articles.json"
) -> List[Dict]:
    """
    Загружает статьи из JSON, классифицирует через LLM и сохраняет результат.
    
    Args:
        input_json: Путь к файлу со статьями (из extract_articles.py)
        output_json: Путь для сохранения отфильтрованных статей
    
    Returns:
        Список отфильтрованных статей (капитальные расходы)
    """
    # Загружаем статьи
    with open(input_json, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    articles = data.get('articles', [])
    years = data.get('years', [])
    
    print(f"Загружено статей: {len(articles)}")
    
    # Классифицируем
    classifier = LLMClassifier()
    filtered, results = classifier.filter_capital_articles(articles)
    
    # Сохраняем лог
    classifier.save_classification_log(results)
    
    # Сохраняем отфильтрованные статьи
    os.makedirs(os.path.dirname(output_json), exist_ok=True)
    
    output_data = {
        'years': years,
        'articles': filtered,
        'classification_stats': classifier.stats
    }
    
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"\nОтфильтрованные статьи сохранены: {output_json}")
    print(f"  Капитальных статей: {len(filtered)}")
    
    return filtered


if __name__ == "__main__":
    import sys
    
    input_file = sys.argv[1] if len(sys.argv) > 1 else "output/extracted_articles.json"
    
    if os.path.exists(input_file):
        classify_from_json(input_file)
    else:
        print(f"Файл {input_file} не найден.")
        print("Сначала запустите extract_articles.py для извлечения статей.")

