# Hentai Diffusion Rebuilt

Новая сборка Discord-бота для ComfyUI, пересобранная с нуля поверх структуры старого проекта, но с более чистой архитектурой.

## Что сохранено
- TXT2IMG
- IMG2IMG
- IMG2VID
- upscale
- дневные лимиты
- лимиты для IMG2VID
- проверка ролей и членства в access guild
- блок-лист и донорские аккаунты
- пресеты промтов, моделей и LoRA
- сиды
- workflow settings через `config(...)` и другие пользовательские хуки
- список workflow'ов
- `/profile`, `/limits`, `/cancel`
- spoiler tags
- кнопки Cancel / Reuse request

## Что улучшено
- логика разделена на сервисы: конфиг, лимиты, безопасность, workflow, очереди, ComfyUI, UI
- очереди и приоритеты вынесены из монолитного класса
- конфиг поддерживает старую структуру и новую
- очередь теперь может работать с несколькими worker'ами
- входные картинки принимаются в PNG/JPEG/WEBP и автоматически переводятся в PNG
- отдельные YAML/JSON хранилища для usage и конфига
- все ключевые ограничения вынесены в конфигурацию

## Быстрый старт
1. Установи зависимости:
   `pip install -r requirements.txt`

2. Скопируй `config.example.yml` в `config.yml`

3. Заполни `discord.token`

4. Запусти:
   `python app.py`

## Структура
- `reborn_bot/config.py` — загрузка и валидация конфига
- `reborn_bot/services/workflows.py` — загрузка и подготовка workflow
- `reborn_bot/services/comfy.py` — клиент ComfyUI
- `reborn_bot/services/security.py` — уровни ролей и разрешения
- `reborn_bot/services/usage.py` — лимиты и статистика
- `reborn_bot/services/queueing.py` — priority queue
- `reborn_bot/bot.py` — Discord-бот и orchestration
