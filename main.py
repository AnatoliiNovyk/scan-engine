import asyncio
import sys
# Імпортуємо функції з наших модулів
from src.async_scanner import main_async_scanner
# Змінено: імпортуємо initialize_es_client та close_es_client
from src.data_ingester import ingest_scan_results, create_index_if_not_exists, initialize_es_client, close_es_client, es as data_ingester_es 

async def main():
    print("--- Запуск ScanEngine ---")

    # 1. Ініціалізація та перевірка індексу Elasticsearch
    print("Ініціалізація клієнта Elasticsearch та перевірка індексу...")
    if not await initialize_es_client(): # Викликаємо нову асинхронну функцію ініціалізації
        print("Критична помилка: Не вдалося підготувати Elasticsearch. Завершення роботи.")
        sys.exit(1)
    
    if not await create_index_if_not_exists():
        print("Критична помилка: Не вдалося створити/перевірити індекс Elasticsearch. Завершення роботи.")
        sys.exit(1)

    # 2. Ініціалізація спільних черг
    scan_results_queue = asyncio.Queue() # Черга для передачі результатів від сканера до ingester'а
    
    # 3. Налаштування параметрів сканування
    # Важливо: використовуйте IP-діапазони, які ви маєте право сканувати!
    # Для тестування можна використовувати: ["127.0.0.1/30"] або ["192.168.1.0/29"] для невеликих локальних мереж.
    ip_ranges = ["127.0.0.1/30"] 
    # Список портів, які потрібно сканувати. Можна також передати None, щоб використати порти за замовчуванням у scanner.py
    ports = [22, 80, 443, 8080] 
    max_scanner_workers = 100 # Кількість одночасних задач сканування
    max_ingester_workers = 5  # Кількість одночасних задач введення даних в Elasticsearch

    # 4. Запуск робітників модуля введення даних (ingester'ів)
    ingester_tasks = []
    print(f"Запускаємо {max_ingester_workers} робітників введення даних...")
    for _ in range(max_ingester_workers):
        task = asyncio.create_task(ingest_scan_results(scan_results_queue))
        ingester_tasks.append(task)
    
    # 5. Запуск модуля сканування
    print(f"Запускаємо модуль сканування для діапазонів {ip_ranges}...")
    scanner_task = asyncio.create_task(
        main_async_scanner(ip_ranges, ports, max_scanner_workers, scan_results_queue)
    )

    # 6. Очікування завершення сканування
    await scanner_task # Чекаємо, поки сканер завершить свою роботу
    print("Модуль сканування завершив збір даних.")

    # 7. Сигнал про завершення для ingester'ів
    print("Відправляємо сигнали завершення робітникам введення даних...")
    for _ in range(max_ingester_workers):
        await scan_results_queue.put(None) # Поміщаємо сигнал None для кожного робітника-ingester'а

    # 8. Очікування, поки всі результати будуть оброблені та проіндексовані
    print("Очікуємо, поки всі дані будуть проіндексовані...")
    await scan_results_queue.join() # Чекаємо, поки всі завдання в черзі будуть позначені як done
    await asyncio.gather(*ingester_tasks, return_exceptions=True) # Чекаємо завершення самих ingester'ів

    print("--- Система ScanEngine завершила роботу. Всі дані оброблені та проіндексовані. ---")
    print("Тепер ви можете запустити веб-інтерфейс, виконавши 'python src/api.py' та перейти до http://127.0.0.1:5000/search?q=<ваш_запит>")

if __name__ == "__main__":
    # Для Windows, можливо, знадобиться встановити політику циклу подій
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) 
    try:
        asyncio.run(main())
    finally:
        # Закриваємо клієнт ES після завершення головної функції
        # Цей виклик має бути поза asyncio.run, щоб він міг закрити клієнт.
        # Однак es клієнт міг бути None, якщо ініціалізація не вдалася.
        # Тому перевіряємо це.
        import src.data_ingester # Потрібно імпортувати, щоб отримати доступ до es
        if src.data_ingester.es is not None:
            # Для закриття клієнта потрібен running loop
            # Це може бути складніше закрити після того, як основний цикл закритий.
            # Якщо виникне помилка Unclosed client session, ми її обробимо.
            try:
                loop = asyncio.get_event_loop()
                if not loop.is_running(): # Якщо цикл не запущений, створюємо новий тимчасовий
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    new_loop.run_until_complete(src.data_ingester.close_es_client())
                    new_loop.close()
                else: # Якщо цикл запущений (наприклад, у тестуванні), закриваємо у ньому
                    loop.run_until_complete(src.data_ingester.close_es_client())
            except RuntimeError as e:
                print(f"Помилка при закритті ES клієнта: {e}. Можливо, цикл подій вже закритий.")
            except Exception as e:
                print(f"Непередбачена помилка при закритті ES клієнта: {e}")
