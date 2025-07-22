import asyncio
import ipaddress
import socket
import sys

# Функція для асинхронного сканування одного порту
async def scan_port_async(ip_address, port, timeout=5):
    """
    Проводить асинхронне SYN-сканування одного порту і намагається отримати банер.
    Повертає банер, якщо порт відкритий, інакше None.
    """
    try:
        # Створення асинхронного сокету
        reader, writer = await asyncio.open_connection(ip_address, port, timeout=timeout)
        
        # Порт відкритий, намагаємося отримати банер
        try:
            writer.write(b'\n\\r\\n') # Відправка простого запиту для багатьох сервісів
            await writer.drain()
            banner = await asyncio.wait_for(reader.read(1024), timeout=timeout)
            banner = banner.decode('utf-8', errors='ignore').strip()
            return banner
        except asyncio.TimeoutError:
            return f"Порт {port} відкритий (без банера за таймаутом)"
        except Exception as e:
            return f"Порт {port} відкритий (помилка отримання банера: {e})"
        finally:
            writer.close()
            await writer.wait_closed()
    except (ConnectionRefusedError, asyncio.TimeoutError, OSError):
        return None # Порт закритий, фільтрується або таймаут з'єднання
    except Exception as e:
        # print(f"Помилка сканування {ip_address}:{port}: {e}") # Для дебагу
        return None

async def worker(ip_queue, results_queue):
    """
    Асинхронний робітник, який бере IP-адреси з черги, сканує їх порти
    та поміщає результати до черги результатів.
    """
    while True:
        ip_address = await ip_queue.get()
        if ip_address is None: # Сигнал завершення
            ip_queue.task_done()
            break

        ports_to_scan = [22, 80, 443, 8080] # Стандартні порти для початку
        
        print(f"Сканування хоста: {ip_address}")
        for port in ports_to_scan:
            banner = await scan_port_async(ip_address, port)
            if banner:
                result = {
                    'ip': ip_address,
                    'port': port,
                    'banner': banner
                }
                await results_queue.put(result)
                print(f"[Знайдено] {ip_address}:{port} -> {banner[:50]}...") # Обмежити вивід банера
            # else:
            #     print(f"[{ip_address}:{port}] - Закрито/Фільтрується") # Можна закоментувати для чистоти виводу

        ip_queue.task_done()

async def resolve_hostname(hostname):
    """
    Асинхронно перетворює ім'я хоста на IP-адресу.
    Використовує run_in_executor для неблокуючого виклику socket.gethostbyname.
    """
    loop = asyncio.get_running_loop()
    try:
        ip_address = await loop.run_in_executor(None, socket.gethostbyname, hostname)
        return ip_address
    except socket.gaierror:
        print(f"Помилка: Не вдалося перетворити ім'я хоста '{hostname}' на IP-адресу.")
        return None

async def main_async_scanner(ip_ranges_cidr, ports_to_scan_override=None, max_scanner_workers=100, results_queue=None):
    """
    Основна асинхронна функція для сканування діапазонів IP.
    Вона ініціалізує черги та робітників.
    """
    if results_queue is None:
        print("Попередження: results_queue не надано. Результати не будуть передані далі.")
        results_queue = asyncio.Queue() # Створити тимчасову чергу, якщо не передано

    ip_queue = asyncio.Queue()
    all_ips = []

    for ip_entry in ip_ranges_cidr: # Змінено: тепер ip_entry може бути ім'ям хоста або CIDR
        try:
            # Спроба перетворити на IP-мережу/адресу
            network = ipaddress.ip_network(ip_entry, strict=False)
            all_ips.extend([str(ip) for ip in network.hosts()])
        except ValueError:
            # Якщо це не IP-мережа, спробувати перетворити як ім'я хоста
            resolved_ip = await resolve_hostname(ip_entry)
            if resolved_ip:
                # Перетворюємо розпізнаний IP на мережу /32 для сканування
                all_ips.append(str(ipaddress.ip_network(resolved_ip + "/32").network_address))
            else:
                print(f"Пропущено: '{ip_entry}' не є дійсною IP-мережею або іменем хоста.")
                continue # Пропускаємо цю некоректну сутність

    # Видаляємо дублікати IP-адрес
    all_ips = list(set(all_ips))
    if not all_ips:
        print("Не знайдено дійсних IP-адрес для сканування. Завершення.")
        # Поміщаємо сигнали завершення для робітників, щоб вони не зависали
        for _ in range(max_scanner_workers):
            await ip_queue.put(None)
        await asyncio.gather(*[asyncio.create_task(worker(ip_queue, results_queue)) for _ in range(max_scanner_workers)], return_exceptions=True)
        return


    print(f"Ініціалізація сканування для {len(all_ips)} унікальних хостів.")

    # Заповнення черги IP-адресами
    for ip in all_ips:
        await ip_queue.put(ip)

    # Створення пулу асинхронних робітників
    workers = []
    for _ in range(max_scanner_workers):
        worker_task = asyncio.create_task(worker(ip_queue, results_queue))
        workers.append(worker_task)

    # Очікування завершення всіх завдань у черзі IP-адрес
    await ip_queue.join()

    # Сигнали завершення для робітників
    for _ in range(max_scanner_workers):
        await ip_queue.put(None)

    # Очікування, поки всі робітники завершать (хоча б спробують завершити)
    await asyncio.gather(*workers, return_exceptions=True)
    
    # print("Модуль сканування завершив роботу.") # Для дебагу


if __name__ == "__main__":
    # Приклад використання для автономного тестування модуля сканування
    # Уникайте сканування публічних мереж без дозволу.
    # Для тестування можна використовувати:
    # "127.0.0.1/24" (для локальної мережі)
    # "scanme.nmap.org" (якщо потрібно сканувати один публічний хост)
    
    # Створіть тимчасову чергу для результатів, якщо запускаєте окремо
    test_results_queue = asyncio.Queue()

    # Запускаємо сканер. Результати будуть виведені в консоль, а також поміщені в test_results_queue.
    async def run_test_scanner():
        # Змінено: ip_ranges_cidr тепер список, може містити доменні імена
        await main_async_scanner(["scanme.nmap.org"], [22, 80, 443], max_scanner_workers=5, results_queue=test_results_queue)
        # Додатково: вивести те, що було поміщено в чергу
        print("\n--- Зібрані результати (з черги) ---")
        while not test_results_queue.empty():
            print(await test_results_queue.get())

    asyncio.run(run_test_scanner())
