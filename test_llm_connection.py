"""
Тестовый скрипт для проверки соединения с LLM сервером.
Отправляет простой запрос и выводит ответ.
"""

import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

def clean_env_value(value: str) -> str:
    """Убирает лишние кавычки."""
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        value = value[1:-1]
    return value.strip()

# Загружаем настройки из .env или используем дефолтные
BASE_URL = clean_env_value(os.getenv("QWEN_BASE_URL", "http://localhost:8880/v1"))
API_KEY = clean_env_value(os.getenv("QWEN_API_KEY", "sk-dummy-key"))
MODEL_NAME = clean_env_value(os.getenv("QWEN_MODEL_NAME", "default"))


def test_models_endpoint():
    """Проверяет доступность сервера через /models."""
    print(f"\n{'='*60}")
    print("ТЕСТ 1: Проверка доступности сервера")
    print(f"{'='*60}")
    print(f"URL: {BASE_URL}/models")
    
    try:
        response = requests.get(
            f"{BASE_URL}/models",
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=10
        )
        print(f"Статус: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✓ Сервер доступен!")
            print(f"Доступные модели:")
            for model in data.get('data', []):
                print(f"  - {model.get('id', 'unknown')}")
            return True
        else:
            print(f"✗ Ошибка: {response.text[:200]}")
            return False
            
    except requests.exceptions.ConnectionError as e:
        print(f"✗ Не удалось подключиться к серверу!")
        print(f"   Ошибка: {e}")
        return False
    except Exception as e:
        print(f"✗ Неожиданная ошибка: {e}")
        return False


def test_chat_completion():
    """Отправляет тестовый запрос к модели."""
    print(f"\n{'='*60}")
    print("ТЕСТ 2: Тестовый запрос к модели")
    print(f"{'='*60}")
    print(f"URL: {BASE_URL}/chat/completions")
    print(f"Модель: {MODEL_NAME}")
    
    url = f"{BASE_URL}/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    
    # Простой тестовый запрос
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": "Ты полезный ассистент. Отвечай кратко на русском языке."},
            {"role": "user", "content": "Привет! Скажи одним предложением, что такое бюджет?"}
        ],
        "temperature": 0.7,
        "max_tokens": 150
    }
    
    print(f"\nОтправка запроса...")
    print(f"Запрос: {payload['messages'][1]['content']}")
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=60
        )
        
        print(f"\nСтатус: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            answer = data['choices'][0]['message']['content']
            print(f"✓ Модель ответила!")
            print(f"\n{'─'*40}")
            print(f"Ответ модели:")
            print(f"{'─'*40}")
            print(answer)
            print(f"{'─'*40}")
            
            # Показываем метаданные если есть
            usage = data.get('usage', {})
            if usage:
                print(f"\nИспользование токенов:")
                print(f"  Входящие: {usage.get('prompt_tokens', 'N/A')}")
                print(f"  Исходящие: {usage.get('completion_tokens', 'N/A')}")
                print(f"  Всего: {usage.get('total_tokens', 'N/A')}")
            
            return True
        else:
            print(f"✗ Ошибка: {response.text[:500]}")
            return False
            
    except requests.exceptions.Timeout:
        print("✗ Таймаут запроса (60 сек)")
        return False
    except Exception as e:
        print(f"✗ Ошибка: {e}")
        return False


def test_budget_classification():
    """Тестирует классификацию бюджетной статьи."""
    print(f"\n{'='*60}")
    print("ТЕСТ 3: Классификация бюджетной статьи")
    print(f"{'='*60}")
    
    url = f"{BASE_URL}/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    
    # Тестовые статьи бюджета
    test_articles = [
        "Строительство спортивной площадки",
        "Содержание дорог общего пользования",
        "Капитальный ремонт школы №5",
        "Выплата зарплаты работникам администрации"
    ]
    
    system_prompt = """Ты эксперт по анализу бюджетных документов.
Определи, относятся ли статьи к КАПИТАЛЬНЫМ ЗАТРАТАМ.
Капитальные: строительство, реконструкция, капремонт, модернизация.
Не капитальные: содержание, текущий ремонт, зарплаты, пособия.

Ответь в формате JSON:
[{"idx": 0, "is_capital": true/false, "reason": "причина"}]"""

    articles_text = "\n".join([f"{i}. {art}" for i, art in enumerate(test_articles)])
    
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Проанализируй статьи:\n{articles_text}"}
        ],
        "temperature": 0.1,
        "max_tokens": 500
    }
    
    print(f"Тестовые статьи:")
    for i, art in enumerate(test_articles):
        print(f"  {i}. {art}")
    
    print(f"\nОтправка запроса...")
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=90
        )
        
        if response.status_code == 200:
            data = response.json()
            answer = data['choices'][0]['message']['content']
            print(f"✓ Получен ответ!")
            print(f"\n{'─'*40}")
            print(f"Ответ модели:")
            print(f"{'─'*40}")
            print(answer)
            print(f"{'─'*40}")
            
            # Пытаемся распарсить JSON
            try:
                # Убираем markdown если есть
                clean = answer.strip()
                if clean.startswith("```"):
                    clean = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
                if clean.endswith("```"):
                    clean = clean[:-3]
                clean = clean.strip()
                
                start = clean.find('[')
                end = clean.rfind(']') + 1
                if start != -1 and end > start:
                    parsed = json.loads(clean[start:end])
                    print(f"\n✓ JSON успешно распознан!")
                    print(f"\nРезультаты классификации:")
                    for item in parsed:
                        status = "✓ КАПИТАЛ" if item.get('is_capital') else "✗ НЕ капитал"
                        idx = item.get('idx', '?')
                        reason = item.get('reason', 'N/A')
                        if isinstance(idx, int) and idx < len(test_articles):
                            print(f"  {status}: {test_articles[idx]} ({reason})")
            except:
                print("\n⚠ Не удалось распарсить JSON (это нормально для теста)")
            
            return True
        else:
            print(f"✗ Ошибка: {response.text[:300]}")
            return False
            
    except Exception as e:
        print(f"✗ Ошибка: {e}")
        return False


def main():
    global BASE_URL, MODEL_NAME
    
    import sys
    
    # Можно передать URL через командную строку
    if len(sys.argv) > 1:
        BASE_URL = sys.argv[1]
        print(f"Используем URL из аргумента: {BASE_URL}")
    if len(sys.argv) > 2:
        MODEL_NAME = sys.argv[2]
    
    print("╔════════════════════════════════════════════════════════════╗")
    print("║       ТЕСТИРОВАНИЕ ПОДКЛЮЧЕНИЯ К LLM СЕРВЕРУ               ║")
    print("╚════════════════════════════════════════════════════════════╝")
    
    print(f"\nТекущие настройки:")
    print(f"  BASE_URL: {BASE_URL}")
    print(f"  MODEL: {MODEL_NAME}")
    print(f"  API_KEY: {API_KEY[:10]}..." if len(API_KEY) > 10 else f"  API_KEY: {API_KEY}")
    
    results = []
    
    # Тест 1: Доступность сервера
    results.append(("Доступность сервера", test_models_endpoint()))
    
    # Тест 2: Простой запрос (только если сервер доступен)
    if results[0][1]:
        results.append(("Chat completion", test_chat_completion()))
    
    # Тест 3: Классификация (только если предыдущие тесты прошли)
    if len(results) > 1 and results[1][1]:
        results.append(("Классификация бюджета", test_budget_classification()))
    
    # Итоги
    print(f"\n{'='*60}")
    print("ИТОГИ ТЕСТИРОВАНИЯ")
    print(f"{'='*60}")
    
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")
    
    passed_count = sum(1 for _, p in results if p)
    total_count = len(results)
    
    print(f"\nПройдено тестов: {passed_count}/{total_count}")
    
    if passed_count == total_count and total_count > 0:
        print("\n✓ Все тесты пройдены! LLM сервер работает корректно.")
    elif passed_count > 0:
        print("\n⚠ Некоторые тесты не прошли. Проверьте настройки.")
    else:
        print("\n✗ Сервер недоступен. Проверьте:")
        print("  1. Запущен ли LLM сервер?")
        print("  2. Правильный ли адрес в QWEN_BASE_URL?")
        print("  3. Открыт ли порт в файрволе?")
        print("  4. Находитесь ли вы в одной сети с сервером?")


if __name__ == "__main__":
    main()

