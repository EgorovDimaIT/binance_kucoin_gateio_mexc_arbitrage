# binance_kucoin_gateio_mexc_arbitrage
|| ||
|| РУКОВОДСТВО ПО АРБИТРАЖНОМУ БОТУ ДЛЯ КРИПТОВАЛЮТ ||
|| ||
ОПИСАНИЕ
Этот скрипт представляет собой сложного бота для межбиржевого арбитража криптовалют. Его основная цель — находить и использовать разницу в ценах на одни и те же активы на разных криптовалютных биржах (Binance, KuCoin, MEXC, Gate.io).
Бот выполняет полный цикл арбитражной сделки:
Сканирование: Постоянно отслеживает цены на общие торговые пары на всех подключенных биржах.
Анализ: При обнаружении потенциальной возможности рассчитывает чистую прибыль с учетом торговых комиссий, комиссий за вывод средств и стабильности цены.
Проверка ликвидности: Убеждается, что на обеих биржах достаточно ликвидности для выполнения сделки без значительного проскальзывания цены.
Исполнение:
Покупает актив на бирже с более низкой ценой.
Переводит актив на биржу с более высокой ценой, выбирая оптимальную (самую дешевую и быструю) сеть для перевода.
Продает актив на второй бирже.
Управление балансами: Может автоматически пополнять баланс USDT на нужной бирже ("JIT-финансирование") и консолидировать "пыль" (мелкие остатки разных токенов) в USDT.
ОСНОВНЫЕ КОМПОНЕНТЫ (КЛАССЫ)
Config: Центральный класс для всех настроек. Это самый важный файл для пользователя. Здесь вы указываете API-ключи, торговые лимиты, пороги прибыли, черные списки активов и сетей.
Scanner: Отвечает за обнаружение начальных "грязных" арбитражных возможностей (без учета комиссий).
Analyzer: Проверяет возможности от сканера. Рассчитывает чистую прибыль, находит наилучшую сеть для перевода, проверяет стабильность и ликвидность.
BalanceManager: Управляет получением данных о балансах со всех бирж и их суб-счетов (спотовый, основной, торговый и т.д.).
Rebalancer: Выполняет переводы активов между биржами и между внутренними счетами биржи. Также отвечает за конвертацию мелких активов в USDT.
Executor: Управляет полным циклом исполнения арбитражной сделки: покупка, ожидание перевода и продажа.
КОНФИГУРАЦИЯ (КЛАСС Config)
Перед запуском бота ОБЯЗАТЕЛЬНО настройте параметры в классе Config.
API_KEYS:
Заполните 'apiKey', 'secret' (и 'password' для KuCoin) для каждой биржи, которую хотите использовать.
БЕЗОПАСНОСТЬ: Никогда не делитесь своими API-ключами. Храните их в безопасности.
DRY_RUN:
DRY_RUN = True: Режим "сухого запуска". Бот будет имитировать все действия (покупку, продажу, переводы), не используя реальные средства. КРАЙНЕ РЕКОМЕНДУЕТСЯ для тестирования.
DRY_RUN = False: Реальный режим. Бот будет торговать вашими реальными деньгами. Используйте с предельной осторожностью.
FEES_DATA_FILE_PATHS:
Укажите путь к текстовому файлу с данными о комиссиях и сетях для вывода. Пример: r"D:\crypto_trader\arbitrage_bot\token_exchange_details_from_bot_config1.txt".
Этот файл позволяет боту использовать точные и актуальные данные о комиссиях, которые могут быть недоступны через API.
Торговые параметры:
TRADE_AMOUNT_USD: Сумма в USDT, которую бот будет использовать для одной арбитражной сделки.
MIN_PROFIT_THRESHOLD_NET: Минимальный процент чистой прибыли, при котором бот начнет сделку (например, Decimal("0.4") означает 0.4%).
MAX_PROFIT_THRESHOLD: Максимальный процент "грязной" прибыли. Используется для отсеивания аномальных, скорее всего ложных, возможностей.
MIN_LIQUIDITY_USD: Минимальная ликвидность в стакане ордеров (в долларах), необходимая для рассмотрения возможности.
Черные и белые списки:
ASSET_UNAVAILABLE_BLACKLIST: Полностью запрещает работу с определенным токеном на указанной бирже (или на всех). Полезно для проблемных активов.
ARB_PATH_BLACKLIST: Запрещает конкретный арбитражный путь (Токен, Биржа покупки, Биржа продажи, Сеть).
TOKEN_NETWORK_RESTRICTIONS: Позволяет жестко задать, какую сеть использовать для вывода определенного токена с определенной биржи (например, выводить USDT с Binance только через BEP20).
ФАЙЛ С ДАННЫМИ О КОМИССИЯХ
Бот использует внешний текстовый файл для загрузки детальной информации о сетях вывода. Формат файла следующий:
Generated code
Токен: BTC
  Биржа: binance
    - Сеть ID: BTC (Имя: BTC)
        Активна: true
        Депозит разрешен: true
        Вывод разрешен: true
        Мин. депозит: 0.00000546
        Комиссия за вывод: 0.000005
        Мин. сумма вывода: 0.001
        Точность вывода (для суммы): 8
    - Сеть ID: BEP20 (Имя: BNB Smart Chain (BEP20))
        Активна: true
        Депозит разрешен: true
        Вывод разрешен: true
...
--------------------------------------------------
Токен: ETH
  Биржа: gateio
    - Сеть ID: ETH (Имя: Ethereum)
        Активна: true
...
Use code with caution.
Бот парсит этот файл для получения точных комиссий, минимальных сумм вывода и статуса доступности сетей.
КАК ЗАПУСТИТЬ
Установите зависимости:
Generated code
pip install ccxt aiohttp
Use code with caution.
Настройте Config: Откройте скрипт и тщательно заполните все необходимые поля в классе Config, особенно API_KEYS и FEES_DATA_FILE_PATHS.
Подготовьте файл с комиссиями: Убедитесь, что файл, указанный в FEES_DATA_FILE_PATHS, существует и имеет правильный формат.
Запустите бота:
Generated code
python <имя_вашего_скрипта>.py
Use code with caution.
Начните с DRY_RUN = True: Запустите бота в режиме симуляции, чтобы убедиться, что все работает корректно, и изучите логи.
ГЕНЕРИРУЕМЫЕ ФАЙЛЫ
complete16_8.log: Основной файл логов. Здесь записывается вся информация о работе бота, включая сканирование, анализ, ошибки и успешные операции.
arbitrage_trade_history_full_cycle.jsonl: История всех совершенных (или попыток совершения) арбитражных сделок в формате JSON Lines. Содержит подробную информацию о каждой стадии сделки.
ВАЖНОЕ ПРЕДУПРЕЖДЕНИЕ
Торговля криптовалютами сопряжена с высоким риском. Вы можете потерять все свои средства.
Автоматизированные торговые боты могут содержать ошибки или вести себя непредсказуемо из-за изменений в API бирж, проблем с сетью или экстремальной волатильности рынка.
Автор скрипта не несет ответственности за любые финансовые потери. Используйте этот бот на свой страх и риск.
НИКОГДА не запускайте бота с реальными деньгами (DRY_RUN = False), не протестировав его тщательно в режиме симуляции и не осознав полностью все риски.
================================================================================
--- ENGLISH VERSION ---
================================================================================
|| ||
|| CRYPTOCURRENCY ARBITRAGE BOT GUIDE ||
|| ||
DESCRIPTION
This script is a sophisticated inter-exchange cryptocurrency arbitrage bot. Its primary purpose is to find and exploit price differences for the same assets across various crypto exchanges (Binance, KuCoin, MEXC, Gate.io).
The bot executes the full arbitrage trade cycle:
Scanning: Continuously monitors prices for common trading pairs on all connected exchanges.
Analysis: When a potential opportunity is found, it calculates the net profit, accounting for trading fees, withdrawal fees, and price stability.
Liquidity Check: Ensures there is sufficient liquidity on both exchanges to execute the trade without significant price slippage.
Execution:
Buys the asset on the exchange with the lower price.
Transfers the asset to the exchange with the higher price, selecting the optimal (cheapest and fastest) network for the transfer.
Sells the asset on the second exchange.
Balance Management: Can automatically fund the required USDT balance on an exchange ("JIT Funding") and consolidate "dust" (small balances of various tokens) into USDT.
CORE COMPONENTS (CLASSES)
Config: The central class for all settings. This is the most important file for the user. Here you specify API keys, trading limits, profit thresholds, and blacklists for assets and networks.
Scanner: Responsible for finding initial "gross" arbitrage opportunities (without accounting for fees).
Analyzer: Vets the opportunities from the scanner. It calculates net profit, finds the best transfer network, and checks for stability and liquidity.
BalanceManager: Manages fetching balance data from all exchanges and their respective sub-accounts (spot, funding, trading, etc.).
Rebalancer: Executes asset transfers between exchanges and between an exchange's internal accounts. It also handles the conversion of small assets into USDT.
Executor: Manages the full execution cycle of an arbitrage trade: buy, wait for transfer, and sell.
CONFIGURATION (Config Class)
Before running the bot, you MUST configure the parameters in the Config class.
API_KEYS:
Fill in the 'apiKey', 'secret' (and 'password' for KuCoin) for each exchange you intend to use.
SECURITY: Never share your API keys. Store them securely.
DRY_RUN:
DRY_RUN = True: Dry Run Mode. The bot will simulate all actions (buying, selling, transfers) without using real funds. HIGHLY RECOMMENDED for testing.
DRY_RUN = False: Live Mode. The bot will trade with your real money. Use with extreme caution.
FEES_DATA_FILE_PATHS:
Specify the path to the text file containing withdrawal fee and network data. Example: r"D:\crypto_trader\arbitrage_bot\token_exchange_details_from_bot_config1.txt".
This file allows the bot to use precise and up-to-date fee information that might not be available via the API.
Trading Parameters:
TRADE_AMOUNT_USD: The amount in USDT the bot will use for a single arbitrage trade.
MIN_PROFIT_THRESHOLD_NET: The minimum net profit percentage required for the bot to initiate a trade (e.g., Decimal("0.4") means 0.4%).
MAX_PROFIT_THRESHOLD: The maximum gross profit percentage. Used to filter out anomalous, likely false, opportunities.
MIN_LIQUIDITY_USD: The minimum liquidity in the order book (in USD) required to consider an opportunity.
Blacklists and Whitelists:
ASSET_UNAVAILABLE_BLACKLIST: Completely disables trading of a specific token on a given exchange (or all). Useful for problematic assets.
ARB_PATH_BLACKLIST: Disables a specific arbitrage path (Token, Buy Exchange, Sell Exchange, Network).
TOKEN_NETWORK_RESTRICTIONS: Allows you to enforce the use of a specific network for withdrawing a specific token from a specific exchange (e.g., only withdraw USDT from Binance via BEP20).
FEE DATA FILE
The bot uses an external text file to load detailed withdrawal network information. The file format is as follows:
Generated code
Токен: BTC
  Биржа: binance
    - Сеть ID: BTC (Имя: BTC)
        Активна: true
        Депозит разрешен: true
        Вывод разрешен: true
        Мин. депозит: 0.00000546
        Комиссия за вывод: 0.000005
        Мин. сумма вывода: 0.001
        Точность вывода (для суммы): 8
    - Сеть ID: BEP20 (Имя: BNB Smart Chain (BEP20))
        Активна: true
        Депозит разрешен: true
        Вывод разрешен: true
...
--------------------------------------------------
Токен: ETH
  Биржа: gateio
    - Сеть ID: ETH (Имя: Ethereum)
        Активна: true
...
Use code with caution.
(Note: The keys are in Russian, but the parser maps them to English equivalents.)
The bot parses this file to get accurate fees, minimum withdrawal amounts, and network availability statuses.
HOW TO RUN
Install Dependencies:
Generated code
pip install ccxt aiohttp
Use code with caution.
Configure Config: Open the script and carefully fill in all necessary fields in the Config class, especially API_KEYS and FEES_DATA_FILE_PATHS.
Prepare the Fee File: Ensure the file specified in FEES_DATA_FILE_PATHS exists and is in the correct format.
Run the Bot:
Generated code
python <your_script_name>.py
Use code with caution.
Start with DRY_RUN = True: Run the bot in simulation mode first to ensure everything works correctly and to study the logs.
GENERATED FILES
complete16_8.log: The main log file. All information about the bot's operation is recorded here, including scanning, analysis, errors, and successful operations.
arbitrage_trade_history_full_cycle.jsonl: A history of all completed (or attempted) arbitrage trades in JSON Lines format. It contains detailed information about each stage of the trade.
IMPORTANT WARNING
Trading cryptocurrencies involves high risk. You could lose all of your funds.
Automated trading bots can contain bugs or behave unpredictably due to changes in exchange APIs, network issues, or extreme market volatility.
The author of this script is not responsible for any financial losses. Use this bot at your own risk.
NEVER run the bot with real money (DRY_RUN = False) without thoroughly testing it in simulation mode and fully understanding all the risks involved.
