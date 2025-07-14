import asyncio
import itertools
import logging
import platform
import sys
from dataclasses import dataclass, field
from decimal import Decimal, getcontext, InvalidOperation, ROUND_DOWN, Context
from typing import Dict, List, Optional, Any, Set, Tuple, Callable, TypeVar, cast
import json
from functools import wraps
import time
import re

# --- Initial Setup ---
try:
    import ccxt # Used for ccxt.NetworkError etc. in Rebalancer specifically
    import ccxt.async_support as ccxt_async
except ImportError:
    print("FATAL ERROR: The 'ccxt' library is not installed. Please run: pip install ccxt aiohttp")
    sys.exit(1)

# Custom Exception
class OrderNotClosedError(ccxt_async.ExchangeError):
    """Custom exception for orders that are not closed when expected."""
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('complete16_8.log', encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ArbitrageMaster")

logging.getLogger("BalanceManager").setLevel(logging.DEBUG)
logging.getLogger("Rebalancer").setLevel(logging.DEBUG)
logging.getLogger("Analyzer").setLevel(logging.DEBUG)
logging.getLogger("Executor").setLevel(logging.DEBUG)
logging.getLogger("Scanner").setLevel(logging.DEBUG)


ctx = Context(prec=28)
getcontext().prec = 28
# --- GLOBAL CONSTANTS ---
DECIMAL_COMPARISON_EPSILON = Decimal('1e-8')
# --- END GLOBAL CONSTANTS ---

USDT_TRANSFER_PRECISION_CONFIG = Decimal('1e-8')
REBALANCER_USDT_TRANSFER_PRECISION = Decimal('1e-6')
REBALANCER_BINANCE_USDT_WITHDRAW_PRECISION = 8


if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

F = TypeVar("F", bound=Callable[..., Any])

def safe_call_wrapper(default_retval: Any = None, error_message_prefix: str = ""):
    def decorator(func: F) -> F:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            func_name = func.__name__
            current_logger = logger
            if args and hasattr(args[0], 'logger') and isinstance(getattr(args[0], 'logger'), logging.Logger):
                current_logger = getattr(args[0], 'logger')
            elif args and isinstance(args[0], type) and hasattr(args[0], '__name__'):
                 pass

            try:
                return await func(*args, **kwargs)
            except (ccxt_async.NetworkError, ccxt_async.RequestTimeout, ccxt_async.DDoSProtection, ccxt_async.ExchangeNotAvailable) as e:
                current_logger.error(f"{error_message_prefix}Network/Exchange issue in '{func_name}': {type(e).__name__} - {e}")
                if default_retval is not None: return default_retval
                raise
            except ccxt_async.InsufficientFunds as e:
                current_logger.error(f"{error_message_prefix}Insufficient funds in '{func_name}': {e}")
                if default_retval is not None: return default_retval
                raise
            except ccxt_async.InvalidOrder as e:
                current_logger.error(f"{error_message_prefix}Invalid order in '{func_name}': {e}")
                if default_retval is not None: return default_retval
                raise
            except ccxt_async.OrderNotFound as e:
                current_logger.warning(f"{error_message_prefix}Order not found in '{func_name}': {e}")
                if default_retval is not None: return default_retval
                return default_retval if default_retval is not None else True
            except OrderNotClosedError as e:
                current_logger.error(f"{error_message_prefix}Order not closed in '{func_name}': {e}")
                if default_retval is not None: return default_retval
                raise
            except ccxt_async.BadRequest as e:
                current_logger.error(f"{error_message_prefix}Bad request in '{func_name}': {e}")
                if default_retval is not None: return default_retval
                raise
            except ccxt_async.AuthenticationError as e:
                current_logger.critical(f"{error_message_prefix}Authentication error in '{func_name}': {e}. Check API keys.")
                if default_retval is not None: return default_retval
                raise
            except ccxt_async.ExchangeError as e:
                current_logger.error(f"{error_message_prefix}Exchange error in '{func_name}': {type(e).__name__} - {e}")
                if default_retval is not None: return default_retval
                raise
            except Exception as e:
                current_logger.exception(f"{error_message_prefix}Unexpected error in async '{func_name}': {type(e).__name__} - {e}")
                if default_retval is not None: return default_retval
                raise

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            func_name = func.__name__
            current_logger = logger
            if args and hasattr(args[0], 'logger') and isinstance(getattr(args[0], 'logger'), logging.Logger):
                current_logger = getattr(args[0], 'logger')
            elif args and isinstance(args[0], type) and hasattr(args[0], '__name__'):
                 pass

            try:
                return func(*args, **kwargs)
            except Exception as e:
                current_logger.exception(f"{error_message_prefix}Unexpected error in sync '{func_name}': {type(e).__name__} - {e}")
                if default_retval is not None: return default_retval
                raise

        return cast(F, async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper)
    return decorator

# --- Data Classes ---
@dataclass
class ArbitrageOpportunity:
    buy_exchange_id: str
    sell_exchange_id: str
    symbol: str
    buy_price: Decimal
    sell_price: Decimal
    gross_profit_pct: Decimal
    stability_count: int = 0
    is_stable: bool = False
    buy_fee_pct: Optional[Decimal] = None
    sell_fee_pct: Optional[Decimal] = None
    withdrawal_fee_usd: Optional[Decimal] = None
    net_profit_pct: Optional[Decimal] = None
    chosen_network: Optional[str] = None
    chosen_network_details: Optional[Dict[str, Any]] = None
    potential_networks: List[Dict[str, Any]] = field(default_factory=list)
    is_liquid_enough_for_trade: bool = False

    @property
    def base_asset(self) -> str: return self.symbol.split('/')[0]
    @property
    def quote_asset(self) -> str: return self.symbol.split('/')[1]
    def get_unique_id(self) -> str: return f"{self.buy_exchange_id}-{self.sell_exchange_id}-{self.symbol}"

@dataclass
class ExchangeBalance:
    exchange_id: str
    total_usd: Decimal = Decimal("0.0")
    assets: Dict[str, Dict[str, Decimal]] = field(default_factory=dict)

@dataclass
class TradeExecutionDetails:
    order_id: Optional[str] = None
    timestamp: Optional[int] = None
    symbol: Optional[str] = None
    side: Optional[str] = None
    price: Optional[Decimal] = None
    amount_base: Optional[Decimal] = None
    cost_quote: Optional[Decimal] = None
    fee_amount: Optional[Decimal] = None
    fee_currency: Optional[str] = None
    status: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None

@dataclass
class CompletedArbitrageLog:
    opportunity_id: str
    timestamp_start: int
    buy_exchange: str
    sell_exchange: str
    symbol: str
    buy_leg: Optional[TradeExecutionDetails] = None
    transfer_leg_details: Optional[Dict[str, Any]] = None
    sell_leg: Optional[TradeExecutionDetails] = None
    initial_buy_cost_usdt: Optional[Decimal] = None
    net_base_asset_after_buy_fee: Optional[Decimal] = None
    base_asset_received_on_sell_exchange: Optional[Decimal] = None
    usdt_received_from_sell: Optional[Decimal] = None
    final_net_profit_usdt: Optional[Decimal] = None
    final_net_profit_pct: Optional[Decimal] = None
    original_opportunity_details: Optional[Dict[str, Any]] = None
    status: str = "PENDING"
    error_message: Optional[str] = None

@dataclass
class RebalanceOperation:
    key: str
    from_exchange: str
    to_exchange: str
    amount: Decimal
    asset: str = "USDT"
    created_at: float = field(default_factory=time.time)

class Config:
    DRY_RUN = False
    JIT_LIQUID_ASSETS_FOR_CONVERSION: Set[str] = {"BTC", "ETH", "BNB", "SOL", "USDC"}
    JIT_FUNDING_MIN_CONVERSION_USD: Decimal = Decimal("1.0")
    API_KEYS = {
        'binance': {'apiKey': '', 'secret': ''},
        'kucoin': {'apiKey': '', 'secret': '', 'password': ''},
        'mexc': {'apiKey': '', 'secret': ''},
        'gateio': {'apiKey': '', 'secret': '',
                   'options': {'createMarketBuyOrderRequiresPrice': False}},
    
    EXCHANGE_BALANCE_QUERY_PARAMS: Dict[str, List[Dict[str, str]]] = {
        'binance': [{'type': 'spot'}, {'type': 'funding'}],
        'kucoin': [{'type': 'main'}, {'type': 'trade'}],
        'mexc': [{}],
        'gateio': [{'account': 'spot'}],
    }
    INTERNAL_TRANSFER_CONFIG: Dict[str, Dict[str, Any]] = {
        'binance': {
            'trading_account_type_str': 'SPOT', 'withdrawal_account_type_str': 'FUNDING',
            'params_for_trading': {'type': 'spot'}, 'params_for_withdrawal': {'type': 'funding'},
            'min_internal_transfer_amount': Decimal('0.000001'), 'withdrawal_wallet_type': 1
        },
        'kucoin': {
            'trading_account_type_str': 'trade', 'withdrawal_account_type_str': 'main',
            'params_for_trading': {'type': 'trade'}, 'params_for_withdrawal': {'type': 'main'},
            'min_internal_transfer_amount': Decimal('0.00000001')
        },
        'mexc': {
            'trading_account_type_str': 'SPOT', 'withdrawal_account_type_str': 'SPOT',
            'params_for_trading': {}, 'params_for_withdrawal': {},
            'min_internal_transfer_amount': Decimal('0.000001')
        },
        'gateio': {
            'trading_account_type_str': 'spot', 'withdrawal_account_type_str': 'spot',
            'params_for_trading': {'account': 'spot'}, 'params_for_withdrawal': {'account': 'spot'},
            'min_internal_transfer_amount': Decimal('0.000001')
        },
    }
    QUOTE_ASSET = "USDT"
    
    # --- ИЗМЕНЕНИЕ 1: Снижаем минимальный порог, чтобы ловить больше реальных сделок ---
    MIN_PROFIT_THRESHOLD_GROSS = Decimal("1.0")
    
    # --- ИЗМЕНЕНИЕ 2: Устанавливаем максимальный порог прибыли в 13% ---
    MAX_PROFIT_THRESHOLD = Decimal("13.0")
    
    MIN_PROFIT_THRESHOLD_NET = Decimal("0.4")
    
    # --- ИЗМЕНЕНИЕ 3: Увеличиваем требование к ликвидности, чтобы отсеять "мусорные" токены ---
    MIN_LIQUIDITY_USD = Decimal("200.0") # Подняли с 50 до 1000

    STABILITY_CYCLES = 1 
    TOP_N_OPPORTUNITIES = 35
    TRADE_AMOUNT_USD = Decimal("10")
    MIN_EFFECTIVE_TRADE_USD = Decimal("10.00")
    RESERVE_BUFFER_USD = Decimal("1.0")
    TRANSFER_FEE_BUFFER_USD = Decimal("1.0")
    JIT_FUNDING_WAIT_S = 660
    BASE_ASSET_TRANSFER_WAIT_S = JIT_FUNDING_WAIT_S * 3
    TRADE_CYCLE_COUNT = 1000
    TRADE_CYCLE_SLEEP_S = 15
    POST_TRADE_COOLDOWN_S = 60
    MIN_REMAINING_USD_TO_LEAVE_ON_EX = Decimal("0.2")
    ASSET_CONSOLIDATION_INTERVAL_CYCLES = 3
    
    FEES_DATA_FILE_PATHS: List[str] = [
        r"D:\crypto_trader\arbitrage_bot\token_exchange_details_from_bot_config1.txt"
    ]
    LOADED_EXCHANGE_FEES_DATA: Dict[str, Dict[str, Any]] = {}
    
    ENFORCE_WHITELIST_ONLY: bool = False

    ASSET_UNAVAILABLE_BLACKLIST: Set[Tuple[str, str]] = {
        ("kucoin", "ACM"), ("binance", "AIMONICA"), ("kucoin", "AIMONICA"),
        ("binance", "AQA"), ("kucoin", "AQA"), ("binance", "ARCA"),
        ("binance", "CLV"), ("binance", "FAKEAI"), ("kucoin", "FAKEAI"),
        ("binance", "HDRO"), ("kucoin", "HDRO"), ("binance", "KISHU"),
        ("kucoin", "KISHU"), ("binance", "NODLPIG"), ("kucoin", "NODLPIG"),
        ("mexc", "NODLPIG"), ("gateio", "NODLPIG"), ("binance", "POLC"),
        ("binance", "RINGAI"), ("kucoin", "RINGAI"), ("binance", "SKYA"),
        ("kucoin", "SKYA"), ("binance", "VELAAI"), ("kucoin", "VELAAI"),
        ("binance", "WHITE"), ("kucoin", "WHITE"), ("kucoin", "COTI"),
        ("binance", "MINT"), ("binance", "VAIX"), ("gateio", "VAIX"),
        ("binance", "BIFIF"), ("mexc", "BIFIF"), ("binance", "BONDLY"),
        ("kucoin", "BONDLY"), ("mexc", "BONDLY"), ("binance", "CREDI"),
        ("mexc", "CREDI"), ("kucoin", "MTL"), ("mexc", "MTL"),
        ("binance", "SMH"), ("mexc", "SMH"), ("binance", "TIDAL"),
        ("mexc", "TIDAL"),

        # --- ДОБАВЛЕНО: Блокировка токена GAMESTOP на всех биржах ---
        ("binance", "GAMESTOP"),
        ("kucoin", "GAMESTOP"),
        ("mexc", "GAMESTOP"),
        ("gateio", "GAMESTOP"),
    }
    
    ARB_PATH_BLACKLIST: Set[Tuple[str, str, str, str]] = {
        ('ACM', 'binance', 'gateio', 'CHILIZ'), 
        ('AQA', 'mexc', 'gateio', 'SOLANA'),   
        ('CLV', 'kucoin', 'gateio', 'ERC20'),  
        ('POLC', 'kucoin', 'mexc', 'BEP20'),  
        ('VELAAI', 'mexc', 'gateio', 'BEP20'),
        ('WHITE', 'gateio', 'mexc', 'ERC20'), 
        ('SAHARA', 'binance', 'kucoin', 'ERC20'), 
        ('SAHARA', 'binance', 'kucoin', 'BEP20'),
        ('SAHARA', 'binance', 'gateio', 'ERC20'),
        ('SAHARA', 'binance', 'gateio', 'BEP20'),
        ('SAHARA', 'mexc', 'gateio', 'ERC20'), 
        ('SAHARA', 'mexc', 'gateio', 'BEP20'),
        ('MINT', 'kucoin', 'mexc', 'SOLANA'), 
        ('MINT', 'kucoin', 'gateio', 'SOLANA'),
        ('BIFIF', 'kucoin', 'gateio', 'ERC20'),
    }

    ARB_WHITELIST: Set[Tuple[str, str, str, str]] = set()
    
    HTX_USDT_WITHDRAWAL_SUSPENDED = True

    NETWORK_ALIASES = {
        'ETH': 'ERC20', 'ETHEREUM': 'ERC20', 'ETH(ERC20)': 'ERC20', 'ERC_20': 'ERC20',
        'BSC': 'BEP20', 'BINANCE_SMART_CHAIN': 'BEP20', 'BNB_SMART_CHAIN': 'BEP20',
        'BEP20(BSC)': 'BEP20', 'BSC(BEP20)': 'BEP20', 'BINANCESMARTCHAIN':'BEP20', 'BNB': 'BEP20',
        'TRX': 'TRC20', 'TRON': 'TRC20', 'TRON(TRC20)': 'TRC20',
        'MATIC': 'POLYGON', 'POLYGON_POS': 'POLYGON', 'MATIC_ERC20': 'POLYGON', 'POLYGON(MATIC)': 'POLYGON',
        'SOL': 'SOLANA',
        'AVAXC': 'AVAX', 'AVAX_C_CHAIN': 'AVAX', 'AVAX_CCHAIN': 'AVAX', 'AVAXCHAIN': 'AVAX', 'AVAX_C': 'AVAX',
        'AVALANCHE_C_CHAIN':'AVAX', 'AVALANCHE':'AVAX', 'AVAXCCHAIN':'AVAX',
        'OPTIMISM': 'OPTIMISM', 'OPTIMISMETH': 'OPTIMISM', 'OP_MAINNET': 'OPTIMISM', 'OPETH': 'OPTIMISM',
        'OP': 'OPTIMISM', 'OPTIMISM(OP)': 'OPTIMISM',
        'ARBITRUM': 'ARBITRUM_ONE', 'ARBITRUMONE': 'ARBITRUM_ONE', 'ARB': 'ARBITRUM_ONE', 'ARBEVM': 'ARBITRUM_ONE',
        'ARBONE': 'ARBITRUM_ONE', 'ARBITRUM_ONE(ARB)': 'ARBITRUM_ONE',
        'BTC': 'BTC', 'BITCOIN': 'BTC', 'SEGWIT': 'BTC',
        'TON': 'TON', 'THE_OPEN_NETWORK': 'TON', 'TONCOIN': 'TON', 'TON2':'TON', 'TON_WITH_MEMO':'TON','TON(THE_OPEN_NETWORK)':'TON',
        'ZKSYNC': 'ZKSYNC_ERA', 'ZKSYNC2': 'ZKSYNC_ERA', 'ZKSYNCERA': 'ZKSYNC_ERA', 'ZKSERA':'ZKSYNC_ERA',
        'LINEA': 'LINEA', 'BASE': 'BASE', 'BASEEVM':'BASE', 'SUI': 'SUI', 'SCROLL': 'SCROLL',
        'APT': 'APTOS', 'APTOS': 'APTOS',
        'STATEMINT': 'ASSET_HUB_POLKADOT', 'ASSET_HUB_(POLKADOT)': 'ASSET_HUB_POLKADOT', 'DOTSM': 'ASSET_HUB_POLKADOT',
        'POLKADOTASSETHUB': 'ASSET_HUB_POLKADOT', 'DOT_SMPOLKADOT':'ASSET_HUB_POLKADOT', 'STATEMINE': 'ASSET_HUB_KUSAMA', 'ASSET_HUB_POLKADOT':'ASSET_HUB_POLKADOT',
        'KCC': 'KCC', 'KUCOIN_COMMUNITY_CHAIN': 'KCC', 'KUCOINCOMMUNITYCHAIN': 'KCC',
        'NEAR': 'NEAR', 'NEAR_PROTOCOL': 'NEAR',
        'GT': 'GATECHAIN', 'GTEVM': 'GATECHAIN', 'GATECHAIN': 'GATECHAIN',
        'KAVAEVM': 'KAVA_EVM', 'KAVA EVM': 'KAVA_EVM', 'KAVA_EVM_CO_CHAIN': 'KAVA_EVM',
        'OPBNB': 'OPBNB', 'CELO': 'CELO', 'CELOGOLD':'CELO', 'XTZ': 'TEZOS', 'EOS': 'EOS', 'VAULTA': 'EOS',
        'KAIA': 'KAIA', 'KLAY':'KAIA', 'KLAYTN':'KAIA', 'MTL_NET': 'MTL_NET', 'MTL': 'MTL_NET', 'MTLETH': 'MTL_NET',
        'CHZ': 'CHILIZ', 'CHZ2': 'CHILIZ', 'KMD_NET': 'KMD_NET', 'KMD': 'KMD_NET', 'kmd': 'KMD_NET',
        'VET_NET': 'VET_NET', 'VET': 'VET_NET', 'CLV_NATIVE': 'CLOVER_NATIVE', 'CLVEVM': 'CLOVER_EVM',
        'CLV': 'CLOVER_NATIVE', 'BERACHAIN': 'BERA', 'BERA_NET': 'BERA', 'BERA': 'BERA',
        'JOY': 'JOYSTREAM', 'JOYSTREAM_NET': 'JOYSTREAM', 'BTCRUNES': 'RUNES',
        'XNA_NET': 'XNA_NET', 'XNA': 'XNA_NET', 'SEIEVM': 'SEI_EVM', 'SEI': 'SEI', 
        'MTRG_NET': 'MTR_NET', 'MTRG': 'MTR_NET', 'MTR_TOKEN': 'MTR_NET', 'MTR': 'MTR_NET',
        'MANGO_NET': 'MANGO', 'MANGO': 'MANGO', 'MGO': 'MANGO', 'mango': 'MANGO',   
        'MINTCHAIN_NET': 'MINT_NET', 'MINTCHAIN': 'MINT_NET', 'MINT_TOKEN_NET': 'MINT_NET', 'MINT': 'MINT_NET',
        'EVMOS_NET': 'EVMOS_NET', 'EVMOS': 'EVMOS_NET', 'EVMOSETH': 'EVMOS_NET', 
        'INJECTIVE': 'INJ', 'INJ': 'INJ', 'ADA': 'ADA', 'ada': 'ADA',
        'NAC': 'NAC', 'GAT': 'NAC', 
        'DEFAULT': 'DEFAULT', 'UNKNOWN_NETWORK': 'UNKNOWN_NETWORK',
    }

    NETWORK_PREFERENCE = [
        'BEP20', 'POLYGON', 'OPTIMISM', 'OPBNB', 'TON', 'AVAX', 'ARBITRUM_ONE',
        'CELO', 'SCROLL', 'APTOS', 'NEAR', 'KAVA_EVM', 'SOLANA', 'KCC',
        'ASSET_HUB_POLKADOT', 'TEZOS', 'GATECHAIN', 'TRC20', 'EOS', 'KAIA', 'ERC20', 'ADA',
        'MTL_NET', 'CHILIZ', 'KMD_NET', 'VET_NET', 'CLOVER_NATIVE', 'CLOVER_EVM',
        'BERA', 'JOYSTREAM', 'RUNES', 'XNA_NET', 'SEI_EVM', 'SEI', 'MTR_NET', 'MANGO', 
        'MINT_NET', 'EVMOS_NET', 'NAC', 'INJ',
        'LIGHTNING', 'SUI', 'ZKLINK', 'STARKNET', 'ZKSYNC_ERA', 'LINEA',
        'MANTLE', 'BLAST', 'BRC20', 'MERLIN', 'XRP', 'XLM', 'ALGO', 'DOT', 'KSM', 'ATOM',
        'LTC', 'DOGE', 'XMR', 'HBAR', 'ONE', 'XDC', 'KAS', 'KDA',
        'RVN', 'SC', 'CRO', 'HECO', 'NEO3', 'ONT', 'THETA', 'ZIL', 'LSK', 'NULS',
        'OAS', 'METIS', 'CORE', 'MODE', 'REEF',
        'HRC20', 'BEP2', 'OMNI', 'BTC',
        'DEFAULT', 'UNKNOWN_NETWORK' 
    ]

    TOKEN_PREFERRED_NETWORKS: Dict[str, List[str]] = {
        'USDT': [
            'OPTIMISM', 'AVAX', 'ARBITRUM_ONE', 'POLYGON', 'TON', 'BEP20', 'OPBNB',
            'APTOS', 'CELO', 'SCROLL', 'NEAR', 'KAVA_EVM', 'SOLANA', 'KCC',
            'ASSET_HUB_POLKADOT', 'TEZOS', 'GATECHAIN', 'TRC20', 'EOS', 'ERC20', 'KAIA'
        ],
        'USDC': ['TRC20', 'BEP20', 'SOLANA', 'POLYGON', 'ARBITRUM_ONE', 'OPTIMISM', 'AVAX', 'ERC20'],
        'ETH': ['ARBITRUM_ONE', 'OPTIMISM', 'POLYGON', 'BEP20', 'ZKSYNC_ERA', 'LINEA', 'ERC20'],
        'BTC': ['LIGHTNING', 'BEP20', 'TRC20', 'BTC'],
        'COTI': ['BEP20', 'ERC20'], 'MTL': ['MTL_NET', 'ERC20'], 
        'HAI': ['VET_NET'], 'ACM': ['CHILIZ'],
        'CLV': ['CLOVER_NATIVE', 'CLOVER_EVM', 'ERC20', 'BEP20'],
        'KMD': ['KMD_NET', 'BEP20'], 'LBR': ['ERC20'], 'OBI': ['BEP20'],
        'RIFSOL': ['SOLANA'], 'SWASH': ['ERC20', 'POLYGON'],
        'TIDAL': ['ERC20'], 'NAVI': ['ERC20'], 'BEAM': ['BEAM'], 
        'CGPU': ['BEP20'], 'GEL': ['ERC20'], 'HDRO': ['INJ'],
        'HOLDSTATION': ['BERA'], 'JOYSTREAM': ['JOYSTREAM'],
        'KOKO': ['SOLANA'], 'KP3R': ['ERC20'], 'LOBO': ['RUNES'],
        'PAW': ['ERC20'], 'POLC': ['BEP20', 'ERC20'], 'RINGAI': ['ERC20'],
        'RLY': ['ERC20'], 'SMURFCAT': ['ERC20'], 'SWAP': ['ERC20'],
        'UMB': ['ERC20'], 'WNXM': ['ERC20'], 'XNA': ['XNA_NET'],
        'TROLL': ['ERC20'], 'UOS': ['ERC20'], 'FISHW': ['SEI_EVM', 'SEI'],
        'MTR': ['MTR_NET'], 'MTRG': ['MTR_NET'], 'THN': ['ERC20'],
        'NETVR': ['ERC20'], 'BSX': ['BASE'], 'GAMESTOP': ['ERC20'],
        'SKYA': ['ERC20'], 'TRADE': ['POLYGON', 'ERC20'],
        'KISHU': ['ERC20'], 'MGO': ['MANGO', 'SOLANA'], 
        'RWA': ['BEP20'], 'HYVE': ['ERC20'], 'CLH': ['ERC20'],
        'OOKI': ['ERC20'], 'STRM': ['BEP20'], 'POLK': ['ERC20'],
        'REVU': ['ADA'], 'XCV': ['BEP20'], 'SAHARA': ['BEP20', 'ERC20'],
        'MINT': ['MINT_NET'], 'PBX': ['ERC20', 'ADA'], 'EVMOS': ['EVMOS_NET'],
        'DEFAI': ['SOLANA'], 'UPO': ['POLYGON'], 'GAT': ['NAC'], 
    }
    TOKEN_NETWORK_RESTRICTIONS: Dict[Tuple[str, str], List[str]] = {
        ('binance', 'USDT'): ['BEP20']  # Разрешить вывод USDT с Binance ТОЛЬКО через сеть BEP20 (BSC)
    }

    ESTIMATED_ASSET_PRICES_USD: Dict[str, Decimal] = {
        "USDT": Decimal("1.0"), "USDC": Decimal("1.0"), "DAI": Decimal("1.0"), "FDUSD": Decimal("1.0"),
        "ETH": Decimal("3500.0"), "BTC": Decimal("65000.0"), "BNB": Decimal("600.0"), "SOL": Decimal("150.0"),
        "MTL": Decimal("0.65"), "HAI": Decimal("0.0095"), "MLT": Decimal("0.0106"),
        "LUNA": Decimal("0.135"), "BONK": Decimal("0.000011"), "KLV": Decimal("0.003"),
        "SNS": Decimal("0.1"), "MAN": Decimal("0.02"), "COTI": Decimal("0.1"), "KMD": Decimal("0.3"),
        "LBR": Decimal("0.2"), "OBI": Decimal("0.005"), "SWASH": Decimal("0.015"), "BEAM": Decimal("0.03"),
        "CGPU": Decimal("0.3"), "HDRO": Decimal("0.05"), "HOLDSTATION": Decimal("1.0"), "JOYSTREAM": Decimal("0.01"),
        "KP3R": Decimal("70.0"), "LOBO": Decimal("0.00001"), "POLC": Decimal("0.01"), "RINGAI": Decimal("0.1"),
        "SWAP": Decimal("0.15"), "UMB": Decimal("0.02"), "WNXM": Decimal("30.0"), "SRM": Decimal("0.04"),
        "GEL": Decimal("0.5"), "ACM": Decimal("2.5"), "CLV": Decimal("0.03"), "TIDAL": Decimal("0.0002"), "NAVI": Decimal("0.05"),
        "KOKO": Decimal("0.001"), "PAW": Decimal("0.0000002"), "RLY": Decimal("0.01"), "SMURFCAT": Decimal("0.00003"),
        "XNA": Decimal("0.003"), "TROLL": Decimal("0.00000003"), "UOS": Decimal("0.15"), "FISHW": Decimal("0.02"),
        "MTR": Decimal("1.0"), "MTRG": Decimal("1.0"), "THN": Decimal("0.002"), "NETVR": Decimal("0.04"), "BSX": Decimal("0.0005"),
        "GAMESTOP": Decimal("0.005"), "SKYA": Decimal("0.1"), "TRADE": Decimal("0.07"), "KISHU": Decimal("0.0000000004"),
        "MGO": Decimal("0.02"), "RWA": Decimal("0.2"), "HYVE": Decimal("0.03"), "CLH": Decimal("0.01"),
        "OOKI": Decimal("0.002"), "STRM": Decimal("0.015"), "POLK": Decimal("0.03"), "REVU": Decimal("0.02"),
        "XCV": Decimal("0.004"), "SAHARA": Decimal("0.1"), "MINT": Decimal("0.001"), "PBX": Decimal("0.003"),
        "EVMOS": Decimal("0.03"), "DEFAI": Decimal("0.1"), "UPO": Decimal("0.05"), "GAT": Decimal("0.02"),
        "ADA": Decimal("0.40"),
    }

    ESTIMATED_WITHDRAWAL_FEES_USD: Dict[str, Dict[str, Decimal]] = {
        'USDT': {
            'ERC20': Decimal("5.0"), 'TRC20': Decimal("1.0"), 'BEP20': Decimal("0.3"),
            'POLYGON': Decimal("0.5"), 'SOLANA': Decimal("1.0"), 'OPTIMISM': Decimal("0.5"),
            'ARBITRUM_ONE': Decimal("0.5"), 'AVAX': Decimal("0.5"), 'TON': Decimal("0.5"),
            'DEFAULT': Decimal("2.0")
        },
        'MTL': {'DEFAULT': Decimal("1.0"), 'MTL_NET': Decimal("0.5"), 'ERC20': Decimal("3.0")},
        'COTI': {'BEP20': Decimal("0.03"), 'ERC20': Decimal("3.0")},
        'KMD': {'KMD_NET': Decimal("0.001"), 'BEP20': Decimal("0.05")},
        'HAI': {'VET_NET': Decimal("0.01")},
        'ACM': {'CHILIZ': Decimal("0.5")}, 'ADA': {'ADA': Decimal("0.1")},
        'DEFAULT_ASSET_FEE': Decimal("0.5") 
    }

    @staticmethod
    def is_leveraged_token(symbol_base: str) -> bool:
        if re.match(r'^[A-Z0-9]{1,10}[1-5][SL]$', symbol_base): return True
        if re.match(r'^[A-Z0-9]{1,10}(UP|DOWN)$', symbol_base, re.IGNORECASE): return True
        if re.match(r'^[A-Z0-9]{1,10}(BULL|BEAR)$', symbol_base, re.IGNORECASE): return True
        if len(symbol_base) > 2 and symbol_base[-1] in ('L', 'S') and symbol_base[-2].isdigit():
            prefix = symbol_base[:-2]
            if prefix.isalpha(): return True
        if len(symbol_base) > 7 and (symbol_base.endswith('L') or symbol_base.endswith('S')) and not symbol_base[:-1].isalpha():
             if re.search(r'\d[LS]$', symbol_base): return True
        return False

    @classmethod
    def normalize_network_name_for_config(cls, network_name_from_api_or_file: Optional[str]) -> str:
        if not network_name_from_api_or_file:
            return "UNKNOWN_NETWORK"
        
        name_to_process = network_name_from_api_or_file.split('/')[0]

        name_upper_for_alias_lookup = name_to_process.upper().strip()
        if name_upper_for_alias_lookup in cls.NETWORK_ALIASES:
            return cls.NETWORK_ALIASES[name_upper_for_alias_lookup]

        name_upper_stripped = name_to_process.upper().strip()
        
        if name_upper_stripped in cls.NETWORK_PREFERENCE and name_upper_stripped not in ['DEFAULT', 'UNKNOWN_NETWORK']:
            return name_upper_stripped

        name_processed_for_alias = re.sub(r'\s*\(.*\)\s*', '', name_upper_stripped).strip()
        name_processed_for_alias = name_processed_for_alias.replace(' ', '_').replace('-', '_')

        if name_processed_for_alias in cls.NETWORK_ALIASES:
            return cls.NETWORK_ALIASES[name_processed_for_alias]
        if name_processed_for_alias in cls.NETWORK_PREFERENCE and name_processed_for_alias not in ['DEFAULT', 'UNKNOWN_NETWORK']:
            return name_processed_for_alias

        aggressive_key = name_upper_stripped.replace(' ', '').replace('-', '').replace('_', '')
        aggressive_key_no_paren = re.sub(r'\(.*\)', '', aggressive_key)
        for alias, standard_name in cls.NETWORK_ALIASES.items():
            normalized_alias_key = alias.upper().replace(' ', '').replace('-', '').replace('_', '')
            normalized_alias_key_no_paren = re.sub(r'\(.*\)', '', normalized_alias_key)
            if aggressive_key == normalized_alias_key or \
               (aggressive_key_no_paren == normalized_alias_key_no_paren and normalized_alias_key_no_paren): 
                return standard_name
        
        if name_processed_for_alias and name_processed_for_alias in cls.NETWORK_PREFERENCE:
            return name_processed_for_alias
        
        final_fallback_name = name_processed_for_alias if name_processed_for_alias else "UNKNOWN_NETWORK"
        return final_fallback_name

    @classmethod
    @safe_call_wrapper(error_message_prefix="Config.load_exchange_fees: ")
    def load_exchange_fees_from_file(cls):
        """
        Parses a structured text file to load exchange fee and network data.
        This method replaces the previous JSON-based loading mechanism.
        """
        cls.LOADED_EXCHANGE_FEES_DATA = {}
        normalized_data: Dict[str, Dict[str, Any]] = {}

        if not cls.FEES_DATA_FILE_PATHS:
            logger.warning("Config.FEES_DATA_FILE_PATHS is not set or empty. No external fee files will be loaded.")
            return
            
        KEY_MAP = {
            'Активна': 'active',
            'Депозит разрешен': 'can_deposit',
            'Вывод разрешен': 'can_withdraw',
            'Мин. депозит': 'min_deposit',
            'Комиссия за вывод': 'fee',
            'Мин. сумма вывода': 'min_withdraw',
            'Точность вывода (для суммы)': 'precision'
        }
        
        def _parse_value(value_str: str) -> Optional[Any]:
            value_str = value_str.strip()
            if value_str.lower() == 'n/a':
                return None
            if value_str.lower() == 'true':
                return True
            if value_str.lower() == 'false':
                return False
            try:
                return Decimal(value_str)
            except (InvalidOperation, TypeError):
                logger.debug(f"Could not parse value '{value_str}' as Decimal, returning as string.")
                return value_str
        
        EXCHANGE_NAME_MAP = {
            'gate': 'gateio'
        }

        def _save_current_network_data(
            data_struct: Dict[str, Dict[str, Any]],
            token: Optional[str],
            exchange_id: Optional[str],
            network_info: Optional[Dict[str, Any]]
        ):
            if not all([token, exchange_id, network_info]):
                return

            original_name = network_info.get('original_name_from_file')
            if not original_name:
                logger.warning(f"Skipping network for {token} on {exchange_id} due to missing original name.")
                return

            normalized_net_name = cls.normalize_network_name_for_config(original_name)
            
            data_struct.setdefault(exchange_id, {})
            data_struct[exchange_id].setdefault(token, {'networks': {}})
            
            final_net_details = {
                'original_name_from_file': original_name,
                'can_withdraw': network_info.get('can_withdraw', False),
                'can_deposit': network_info.get('can_deposit', False),
                'fee': network_info.get('fee'),
                'min_withdraw': network_info.get('min_withdraw', Decimal('0')),
                'min_deposit': network_info.get('min_deposit', Decimal('0')),
                'percentage': False 
            }
            
            if final_net_details['can_withdraw'] and final_net_details['fee'] is not None:
                data_struct[exchange_id][token]['networks'][normalized_net_name] = final_net_details
                logger.debug(f"Saved network '{normalized_net_name}' for {token} on {exchange_id}.")
            else:
                logger.debug(f"Skipping saving network '{normalized_net_name}' for {token} on {exchange_id} (can_withdraw: {final_net_details['can_withdraw']}, fee: {final_net_details['fee']}).")

        for file_path in cls.FEES_DATA_FILE_PATHS:
            logger.info(f"Parsing fee data from text file: {file_path}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    current_token: Optional[str] = None
                    current_exchange: Optional[str] = None
                    current_network_data: Optional[Dict[str, Any]] = None

                    for line in f:
                        line = line.rstrip()
                        if not line.strip() or line.strip().startswith('---'):
                            continue

                        token_match = re.match(r"^Токен:\s*(.*)", line)
                        if token_match:
                            _save_current_network_data(normalized_data, current_token, current_exchange, current_network_data)
                            current_token = token_match.group(1).strip().upper()
                            current_exchange = None
                            current_network_data = None
                            logger.debug(f"--- Parsing new token: {current_token} ---")
                            continue

                        exchange_match = re.match(r"^\s*Биржа:\s*(.*)", line)
                        if exchange_match:
                            _save_current_network_data(normalized_data, current_token, current_exchange, current_network_data)
                            ex_name_from_file = exchange_match.group(1).strip().lower()
                            current_exchange = EXCHANGE_NAME_MAP.get(ex_name_from_file, ex_name_from_file)
                            current_network_data = None
                            logger.debug(f"Parsing exchange: {current_exchange} for token {current_token}")
                            continue

                        network_match = re.match(r"^\s*-\s*Сеть ID:\s*.*?\s*\(Имя:\s*(.*?)\)", line)
                        if network_match:
                            _save_current_network_data(normalized_data, current_token, current_exchange, current_network_data)
                            network_name_raw = network_match.group(1).strip(')')
                            current_network_data = {'original_name_from_file': network_name_raw}
                            logger.debug(f"Parsing network: {network_name_raw}")
                            continue
                        
                        if current_network_data is not None:
                            prop_match = re.match(r"^\s{8,}(.*?):\s*(.*)", line)
                            if prop_match:
                                key_ru, value_raw = prop_match.groups()
                                key_en = KEY_MAP.get(key_ru.strip())
                                if key_en:
                                    parsed_value = _parse_value(value_raw)
                                    current_network_data[key_en] = parsed_value
                                    logger.debug(f"  Parsed prop: {key_en} = {parsed_value}")

                    _save_current_network_data(normalized_data, current_token, current_exchange, current_network_data)

            except FileNotFoundError:
                logger.error(f"Fee file not found: {file_path}. Skipping this file.")
                continue
            except Exception as e:
                logger.error(f"Unexpected error parsing text fee file {file_path}: {e}. Skipping.", exc_info=True)
                continue
        
        cls.LOADED_EXCHANGE_FEES_DATA = normalized_data
        if cls.LOADED_EXCHANGE_FEES_DATA:
            logger.info(f"Successfully parsed and populated fee data from text file for exchanges: {list(cls.LOADED_EXCHANGE_FEES_DATA.keys())}")
        else:
            logger.warning("No fee data was populated from the text file after parsing.")
            
    @classmethod
    def get_network_priority_score(cls, network_normalized: str, asset_symbol: Optional[str] = None) -> int:
        network_upper = network_normalized.upper() 
        token_pref_score = -1

        if asset_symbol:
            asset_upper = asset_symbol.upper()
            if asset_upper in cls.TOKEN_PREFERRED_NETWORKS:
                try:
                    token_pref_score = cls.TOKEN_PREFERRED_NETWORKS[asset_upper].index(network_upper)
                    return token_pref_score 
                except ValueError:
                    pass
        
        offset_for_general = 0
        if asset_symbol and asset_symbol.upper() in cls.TOKEN_PREFERRED_NETWORKS and token_pref_score == -1 :
             offset_for_general = len(cls.TOKEN_PREFERRED_NETWORKS[asset_symbol.upper()])

        try:
            general_pref_score = cls.NETWORK_PREFERENCE.index(network_upper)
            return general_pref_score + offset_for_general + 1000 
        except ValueError:
            logger.debug(f"Network '{network_upper}' for asset '{asset_symbol}' not in specific or general preference lists. Assigning lowest priority.")
            large_offset = 10000 
            if asset_symbol and asset_symbol.upper() in cls.TOKEN_PREFERRED_NETWORKS:
                large_offset += len(cls.TOKEN_PREFERRED_NETWORKS[asset_symbol.upper()])
            large_offset += len(cls.NETWORK_PREFERENCE)
            return large_offset

class BalanceManager:
    def __init__(self, exchanges: Dict[str, ccxt_async.Exchange]):
        self.exchanges = exchanges
        self.logger = logging.getLogger(self.__class__.__name__)
        self.tickers_cache: Dict[str, Any] = {}
        self.cache_timestamp: float = 0.0

    async def _get_tickers_with_cache(self, cache_duration_s: int = 60) -> Dict[str, Any]:
        """
        Получает тикеры с референсной биржи, используя кэш для предотвращения лишних запросов.
        """
        current_time = time.time()
        if self.tickers_cache and (current_time - self.cache_timestamp < cache_duration_s):
            self.logger.debug("Using cached tickers for USD value calculation.")
            return self.tickers_cache

        ref_exchange_id = 'binance'
        if not (self.exchanges.get(ref_exchange_id) and self.exchanges[ref_exchange_id].markets):
            alt_ex = next((ex_id for ex_id, ex in self.exchanges.items() if ex and ex.markets and ex.has.get('fetchTickers')), None)
            if alt_ex:
                ref_exchange_id = alt_ex
                self.logger.info(f"Primary reference exchange 'binance' not available for tickers. Using '{ref_exchange_id}' instead.")
            else:
                self.logger.warning("No suitable exchange found to fetch all tickers. USD values will rely on estimates.")
                return {}
        
        ref_ex_inst = self.exchanges[ref_exchange_id]
        try:
            original_timeout = getattr(ref_ex_inst, 'timeout', 30000)
            ref_ex_inst.timeout = 60000
            tickers = await ref_ex_inst.fetch_tickers()
            ref_ex_inst.timeout = original_timeout
            
            if tickers:
                self.logger.debug(f"Fetched and cached {len(tickers)} tickers from {ref_exchange_id} for USD conversion.")
                self.tickers_cache = tickers
                self.cache_timestamp = current_time
                return tickers
            return {}
        except Exception as e:
            self.logger.error(f"Could not fetch tickers from reference exchange {ref_exchange_id}: {e}", exc_info=True)
            return {}

    @safe_call_wrapper(default_retval={})
    async def get_all_balances(self, calculate_usd_values: bool = True) -> Dict[str, ExchangeBalance]:
        self.logger.debug("Fetching all balances across configured exchanges and account types...")
        balance_tasks = {
            ex_id: asyncio.create_task(self._fetch_single_exchange_aggregated_balance(ex_id))
            for ex_id in self.exchanges.keys()
        }
        task_results = await asyncio.gather(*balance_tasks.values(), return_exceptions=True)
        all_exchange_balances: Dict[str, ExchangeBalance] = {}
        for ex_id, result in zip(balance_tasks.keys(), task_results):
            if isinstance(result, ExchangeBalance):
                all_exchange_balances[ex_id] = result
            elif isinstance(result, Exception):
                self.logger.error(f"Failed to fetch aggregated balance for {ex_id}: {result}")
                all_exchange_balances[ex_id] = ExchangeBalance(exchange_id=ex_id)
            else:
                self.logger.warning(f"Unexpected result type for {ex_id} aggregated balance: {type(result)}. Creating empty.")
                all_exchange_balances[ex_id] = ExchangeBalance(exchange_id=ex_id)
        if calculate_usd_values:
            await self._update_all_usd_values(all_exchange_balances)
        self.logger.debug("Finished fetching and processing all balances.")
        return all_exchange_balances

    @safe_call_wrapper()
    async def _fetch_single_exchange_aggregated_balance(self, exchange_id: str) -> ExchangeBalance:
        self.logger.debug(f"Fetching aggregated balance for {exchange_id}...")
        aggregated_eb = ExchangeBalance(exchange_id=exchange_id)
        ex_inst = self.exchanges.get(exchange_id)
        if not ex_inst:
            self.logger.error(f"Exchange instance for {exchange_id} not found. Returning empty balance object.")
            return aggregated_eb

        query_param_sets = Config.EXCHANGE_BALANCE_QUERY_PARAMS.get(exchange_id, [{}])
        temp_assets_sum: Dict[str, Dict[str, Decimal]] = {}

        for params_set in query_param_sets:
            account_type_description = f" (params: {params_set})" if params_set else " (default params)"
            try:
                original_timeout = getattr(ex_inst, 'timeout', 30000)
                ex_inst.timeout = 60000
                self.logger.debug(f"Fetching balance from {exchange_id}{account_type_description} with timeout {ex_inst.timeout}ms")
                balance_data = await ex_inst.fetch_balance(params=params_set)
                ex_inst.timeout = original_timeout

                info_data = balance_data.get('info')
                
                # --- ИСПРАВЛЕНО: Специализированная и приоритетная обработка для Gate.io ---
                # Для спотового аккаунта Gate.io, поле 'info' является наиболее полным и авторитетным источником.
                # Мы обрабатываем его в первую очередь и пропускаем стандартную обработку, чтобы избежать конфликтов.
                if exchange_id == 'gateio' and params_set.get('account') == 'spot' and isinstance(info_data, list):
                    self.logger.debug(f"Using Gate.io specific 'info' list parser for spot account.")
                    for item in info_data:
                        asset = item.get('currency')
                        if not asset: continue
                        asset_upper = asset.upper()
                        
                        try:
                            available_s = item.get('available', '0')
                            locked_s = item.get('locked', '0')
                            free_d = Decimal(str(available_s))
                            locked_d = Decimal(str(locked_s))
                            total_d = free_d + locked_d

                            if total_d > DECIMAL_COMPARISON_EPSILON:
                                # Просто перезаписываем данные, так как это самый точный источник для gateio
                                temp_assets_sum[asset_upper] = {'free': free_d, 'used': locked_d, 'total': total_d}
                        except (InvalidOperation, TypeError) as e_dec_gate:
                             self.logger.warning(f"Could not parse Gate.io 'info' list balance for {asset_upper}{account_type_description}: available='{available_s}'. Error: {e_dec_gate}")
                    continue # Пропускаем остальную часть цикла, переходим к следующему params_set

                # Стандартная обработка для всех остальных бирж и типов аккаунтов
                total_balances_dict = balance_data.get('total')
                if isinstance(total_balances_dict, dict):
                    for asset, total_amount_str in total_balances_dict.items():
                        asset_upper = asset.upper()
                        try:
                            total_decimal = Decimal(str(total_amount_str))
                            if total_decimal > DECIMAL_COMPARISON_EPSILON:
                                free_str = balance_data.get('free', {}).get(asset, '0')
                                used_str = balance_data.get('used', {}).get(asset, '0')

                                if asset_upper not in temp_assets_sum:
                                    temp_assets_sum[asset_upper] = {'free': Decimal(0), 'used': Decimal(0), 'total': Decimal(0)}

                                temp_assets_sum[asset_upper]['free'] += Decimal(str(free_str if free_str is not None else '0'))
                                temp_assets_sum[asset_upper]['used'] += Decimal(str(used_str if used_str is not None else '0'))
                                temp_assets_sum[asset_upper]['total'] += total_decimal
                        except (InvalidOperation, TypeError) as e_dec:
                            self.logger.warning(f"Could not parse standard balance for {asset_upper} on {exchange_id}{account_type_description}: amount='{total_amount_str}'. Error: {e_dec}")
                
                # Специальная обработка для KuCoin (может дополнять данные из 'total')
                if exchange_id == 'kucoin' and isinstance(info_data, dict) and 'data' in info_data and isinstance(info_data['data'], list):
                    kucoin_account_type_filter = params_set.get('type', '').lower()
                    for item in info_data['data']:
                        asset = item.get('currency')
                        if not asset: continue
                        asset_upper = asset.upper()
                        item_account_type = item.get('type', '').lower()

                        if kucoin_account_type_filter and item_account_type != kucoin_account_type_filter:
                            continue
                        
                        # Если этого актива еще нет в нашем списке, добавляем его
                        if asset_upper not in temp_assets_sum:
                            try:
                                total_s = item.get('balance')
                                if total_s and Decimal(str(total_s)) > DECIMAL_COMPARISON_EPSILON:
                                    free_s = item.get('available', '0')
                                    used_s = item.get('holds', '0')
                                    temp_assets_sum[asset_upper] = {
                                        'free': Decimal(str(free_s)),
                                        'used': Decimal(str(used_s)),
                                        'total': Decimal(str(total_s))
                                    }
                            except (InvalidOperation, TypeError) as e_dec_info:
                                self.logger.warning(f"Could not parse KuCoin 'info' balance for {asset_upper}{account_type_description}: amount='{total_s}'. Error: {e_dec_info}")

            except (ccxt_async.NetworkError, ccxt_async.RequestTimeout, ccxt_async.ExchangeError) as e_fetch:
                self.logger.error(f"Error fetching balance from {exchange_id}{account_type_description}: {e_fetch}", exc_info=False)
            except Exception as e_unexpected:
                self.logger.error(f"Unexpected error during balance fetch for {exchange_id}{account_type_description}: {e_unexpected}", exc_info=True)

        aggregated_eb.assets = {
            asset: details for asset, details in temp_assets_sum.items()
            if details.get('total', Decimal(0)) > DECIMAL_COMPARISON_EPSILON
        }
        if not aggregated_eb.assets:
            self.logger.debug(f"No assets with a positive balance found for {exchange_id} after checking all account types.")
        return aggregated_eb

    async def _update_all_usd_values(self, all_balances: Dict[str, ExchangeBalance]):
        self.logger.debug("Attempting to update USD values for all fetched balances...")
        tickers = await self._get_tickers_with_cache()
        quote_asset_upper = Config.QUOTE_ASSET.upper()
        stablecoins_set = {'USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'FDUSD', 'USDP', 'AEUR', 'USTC', 'PYUSD', 'USDE'}

        for ex_id, balance_obj in all_balances.items():
            if not balance_obj or not balance_obj.assets:
                if balance_obj: balance_obj.total_usd = Decimal("0.0")
                continue

            current_exchange_total_usd = Decimal("0.0")
            exchange_instance = self.exchanges.get(ex_id)

            for asset_code, amounts_dict in balance_obj.assets.items():
                asset_code_upper = asset_code.upper()
                asset_total_amount = amounts_dict.get('total', Decimal(0))
                asset_usd_value = Decimal("0.0")
                price_found = False

                if asset_code_upper == quote_asset_upper or asset_code_upper in stablecoins_set:
                    asset_usd_value = asset_total_amount
                    price_found = True
                else:
                    pair_symbol = f"{asset_code_upper}/{quote_asset_upper}"
                    ticker_data = tickers.get(pair_symbol)
                    price_from_ticker = None

                    if ticker_data and ticker_data.get('last') is not None:
                        try:
                            price_from_ticker = Decimal(str(ticker_data['last']))
                            if price_from_ticker > 0:
                                asset_usd_value = asset_total_amount * price_from_ticker
                                price_found = True
                                self.logger.debug(f"Priced {pair_symbol} from general ticker cache: {price_from_ticker}")
                        except (InvalidOperation, TypeError):
                            price_from_ticker = None
                    
                    if not price_found and exchange_instance and exchange_instance.has.get('fetchTicker'):
                        try:
                            self.logger.debug(f"Price for {pair_symbol} not in general cache. Fetching directly from {ex_id}...")
                            local_ticker = await exchange_instance.fetch_ticker(pair_symbol)
                            local_price_str = local_ticker.get('last') or local_ticker.get('bid')
                            if local_price_str:
                                local_price = Decimal(str(local_price_str))
                                if local_price > 0:
                                    asset_usd_value = asset_total_amount * local_price
                                    price_found = True
                                    self.logger.debug(f"Priced {pair_symbol} from local ticker on {ex_id}: {local_price}")
                        except Exception:
                            self.logger.debug(f"Could not fetch local ticker for {pair_symbol} on {ex_id}.")
                    
                    if not price_found:
                        estimated_price = Config.ESTIMATED_ASSET_PRICES_USD.get(asset_code_upper)
                        if estimated_price:
                            asset_usd_value = asset_total_amount * estimated_price
                            self.logger.warning(f"Used ESTIMATED_ASSET_PRICES_USD for {asset_code_upper} on {ex_id}: ${estimated_price}")
                        else:
                            self.logger.error(f"No live ticker or estimated price found for {pair_symbol} (or {asset_code_upper}). USD value for this asset will be 0.")
                
                amounts_dict['usd_value'] = asset_usd_value.quantize(Decimal('0.01'), rounding=ROUND_DOWN)
                current_exchange_total_usd += asset_usd_value
                if asset_usd_value > Decimal("0.01"):
                     self.logger.debug(f"{ex_id}: {asset_code_upper} = {asset_total_amount:.8f} (~${asset_usd_value:,.2f}) (Free: {amounts_dict.get('free',0):.8f}, Used: {amounts_dict.get('used',0):.8f})")

            balance_obj.total_usd = current_exchange_total_usd.quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            if balance_obj.total_usd > Decimal("0.01") or balance_obj.assets :
                self.logger.info(f"Exchange [{ex_id.upper()}] Total Estimated Balance: ${balance_obj.total_usd:,.2f}")
        self.logger.debug("Finished updating USD values for all balances.")

    @safe_call_wrapper()
    async def get_specific_account_balance(self, exchange_instance: ccxt_async.Exchange, asset: str,
                                           account_description: str,
                                           balance_query_params: Dict[str, Any]
                                           ) -> Optional[Decimal]:
        asset_upper = asset.upper()
        self.logger.debug(f"Fetching FREE balance for {asset_upper} on {exchange_instance.id}, account: '{account_description}' (params: {balance_query_params})")
        try:
            original_timeout = getattr(exchange_instance, 'timeout', 30000)
            exchange_instance.timeout = 60000
            balance_data = await exchange_instance.fetch_balance(params=balance_query_params)
            exchange_instance.timeout = original_timeout

            free_balance_decimal = Decimal(0)

            # --- ИСПРАВЛЕНО: Добавлена более надежная проверка для Gate.io ---
            if exchange_instance.id == 'gateio' and balance_query_params.get('account','').lower() == 'spot' and isinstance(balance_data.get('info'), list):
                 for item in balance_data['info']:
                     if item.get('currency','').upper() == asset_upper:
                         free_balance_decimal = Decimal(str(item.get('available','0')))
                         break # Найден нужный актив
            elif isinstance(balance_data.get('free'), dict) and asset_upper in balance_data['free'] and balance_data['free'][asset_upper] is not None:
                free_balance_decimal = Decimal(str(balance_data['free'][asset_upper]))
            elif asset_upper in balance_data and isinstance(balance_data[asset_upper], dict) and balance_data[asset_upper].get('free') is not None:
                free_balance_decimal = Decimal(str(balance_data[asset_upper]['free']))
            elif exchange_instance.id == 'kucoin' and 'info' in balance_data and isinstance(balance_data.get('info'), dict) and \
                 'data' in balance_data['info'] and isinstance(balance_data['info']['data'], list):
                kucoin_account_type_filter = balance_query_params.get('type', '').lower()
                for item in balance_data['info']['data']:
                    if item.get('currency','').upper() == asset_upper and \
                       (not kucoin_account_type_filter or item.get('type','').lower() == kucoin_account_type_filter):
                        free_balance_decimal = Decimal(str(item.get('available', '0')))
                        break
            elif 'info' in balance_data and isinstance(balance_data.get('info'), dict) and \
                 asset_upper in balance_data['info'] and isinstance(balance_data['info'][asset_upper], dict) and \
                 balance_data['info'][asset_upper].get('free') is not None:
                free_balance_decimal = Decimal(str(balance_data['info'][asset_upper]['free']))
            elif asset_upper in balance_data and isinstance(balance_data[asset_upper], (str, float, int)):
                 self.logger.warning(f"Asset {asset_upper} found directly as a value in balance_data for {exchange_instance.id} with params {balance_query_params}. This usually indicates total, not free. Assuming 0 free for safety.")
                 free_balance_decimal = Decimal(0)
            else:
                self.logger.debug(f"Could not find free balance for {asset_upper} via standard paths or known specific paths for {exchange_instance.id} / {account_description}. Assuming 0.")
                free_balance_decimal = Decimal(0)

            self.logger.debug(f"Fetched FREE {asset_upper} on {exchange_instance.id} ('{account_description}', params {balance_query_params}): {free_balance_decimal:.8f}")
            return free_balance_decimal
        except (InvalidOperation, TypeError) as e_parse:
            self.logger.error(f"Error parsing balance for {asset_upper} on {exchange_instance.id} ('{account_description}'): {e_parse}", exc_info=True)
            return None
        except (ccxt_async.NetworkError, ccxt_async.RequestTimeout, ccxt_async.ExchangeError) as e_ccxt:
            self.logger.error(f"CCXT Error fetching specific balance for {asset_upper} on {exchange_instance.id} ('{account_description}'): {e_ccxt}", exc_info=False)
            return None
        except Exception as e_unexp:
            self.logger.error(f"Unexpected error fetching specific balance for {asset_upper} on {exchange_instance.id} ('{account_description}'): {e_unexp}", exc_info=True)
            return None
         

    def __init__(self, exchanges: Dict[str, ccxt_async.Exchange]):
        self.exchanges = exchanges
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stable_candidates: Dict[str, ArbitrageOpportunity] = {}
        self.currencies_cache: Dict[str, Dict[str, Any]] = {}
        self.balance_manager: Optional['BalanceManager'] = None
        self.executor: Optional['Executor'] = None

    def set_balance_manager(self, balance_manager: 'BalanceManager'):
        self.balance_manager = balance_manager

    def set_executor(self, executor: 'Executor'):
        self.executor = executor

    @safe_call_wrapper(default_retval=None)
    async def get_asset_price_in_usdt(self, asset_code: str,
                                      preferred_ref_exchange_id: str = 'binance',
                                      opportunity_context: Optional[ArbitrageOpportunity] = None) -> Optional[Decimal]:
        asset_code_upper = asset_code.upper()
        quote_asset_upper = Config.QUOTE_ASSET.upper()

        if asset_code_upper == quote_asset_upper:
            return Decimal("1.0")

        exchanges_to_try_order: List[Tuple[str, str]] = []

        if opportunity_context:
            if opportunity_context.buy_exchange_id in self.exchanges:
                exchanges_to_try_order.append((opportunity_context.buy_exchange_id, f"opportunity buy_exchange ({opportunity_context.buy_exchange_id})"))
            if opportunity_context.sell_exchange_id in self.exchanges and \
               opportunity_context.sell_exchange_id != opportunity_context.buy_exchange_id:
                exchanges_to_try_order.append((opportunity_context.sell_exchange_id, f"opportunity sell_exchange ({opportunity_context.sell_exchange_id})"))

        if preferred_ref_exchange_id in self.exchanges and \
           preferred_ref_exchange_id not in [ex_id for ex_id, _ in exchanges_to_try_order]:
            exchanges_to_try_order.append((preferred_ref_exchange_id, f"primary reference ({preferred_ref_exchange_id})"))

        tried_ids_set = {ex_id for ex_id, _ in exchanges_to_try_order}
        for ex_id_fallback, ex_inst_fallback in self.exchanges.items():
            if ex_id_fallback not in tried_ids_set and ex_inst_fallback and \
               hasattr(ex_inst_fallback, 'has') and ex_inst_fallback.has.get('fetchTicker') and ex_inst_fallback.markets:
                exchanges_to_try_order.append((ex_id_fallback, f"fallback ({ex_id_fallback})"))

        if not exchanges_to_try_order:
            self.logger.warning(f"No suitable exchanges available to fetch live price for {asset_code_upper}.")
            est_price = Config.ESTIMATED_ASSET_PRICES_USD.get(asset_code_upper)
            if est_price: self.logger.warning(f"Using ESTIMATED_ASSET_PRICES_USD for {asset_code_upper}: ${est_price}"); return est_price
            self.logger.error(f"Failed to get USD price for {asset_code_upper}: No exchanges and no estimate."); return None

        symbol_to_fetch_vs_usdt = f"{asset_code_upper}/{quote_asset_upper}"
        self.logger.debug(f"Attempting to get USD price for {asset_code_upper}. Order of exchanges to try: {[ex_id for ex_id, _ in exchanges_to_try_order]}")

        for ex_id_to_try, source_description in exchanges_to_try_order:
            exchange_instance = self.exchanges.get(ex_id_to_try)
            if not exchange_instance or not exchange_instance.markets:
                continue

            original_timeout = getattr(exchange_instance, 'timeout', 30000)
            exchange_instance.timeout = 60000
            try:
                if symbol_to_fetch_vs_usdt in exchange_instance.markets:
                    self.logger.debug(f"Fetching ticker for {symbol_to_fetch_vs_usdt} on {exchange_instance.id} (Source: {source_description})")
                    ticker = await exchange_instance.fetch_ticker(symbol_to_fetch_vs_usdt)
                    price_value_str = (ticker.get('last') or ticker.get('ask') or ticker.get('bid') or ticker.get('close')) if ticker else None

                    if price_value_str is not None:
                        price_decimal = Decimal(str(price_value_str))
                        if price_decimal > 0:
                            self.logger.info(f"Live price for {symbol_to_fetch_vs_usdt} obtained from {exchange_instance.id} ({source_description}): {price_decimal}")
                            exchange_instance.timeout = original_timeout; return price_decimal
                        else:
                            self.logger.debug(f"Fetched zero or negative price ({price_decimal}) for {symbol_to_fetch_vs_usdt} from {exchange_instance.id}.")
                    else:
                        self.logger.debug(f"No usable price (last/ask/bid/close) in ticker for {symbol_to_fetch_vs_usdt} from {exchange_instance.id}.")
                else:
                    self.logger.debug(f"Market {symbol_to_fetch_vs_usdt} not listed on {exchange_instance.id} ({source_description}).")
            except (ccxt_async.NetworkError, ccxt_async.RequestTimeout, ccxt_async.ExchangeError) as e_ccxt:
                self.logger.warning(f"CCXT error fetching price for {asset_code_upper} on {exchange_instance.id} ({source_description}): {e_ccxt}")
            except (InvalidOperation, TypeError) as e_parse:
                self.logger.warning(f"Error parsing price for {asset_code_upper} on {exchange_instance.id} ({source_description}): {e_parse}")
            except Exception as e_unexp:
                self.logger.warning(f"Unexpected error fetching price for {asset_code_upper} on {exchange_instance.id} ({source_description}): {e_unexp}", exc_info=True)
            finally:
                if exchange_instance:
                    exchange_instance.timeout = original_timeout

        estimated_price = Config.ESTIMATED_ASSET_PRICES_USD.get(asset_code_upper)
        if estimated_price:
            self.logger.warning(f"Failed to fetch live price for {asset_code_upper} from all sources. Using ESTIMATED_ASSET_PRICES_USD: ${estimated_price}.")
            return estimated_price

        self.logger.error(f"Failed to get any USD price (live or estimated) for {asset_code_upper}.")
        return None

    @safe_call_wrapper(default_retval=None)
    async def analyze_and_select_best(self, opportunities: List[ArbitrageOpportunity]) -> Optional[ArbitrageOpportunity]:
        if not opportunities:
            self.logger.debug("Analyzer: No gross opportunities provided to analyze_and_select_best.")
            return None

        valid_opportunities_pre_stability = []
        for opp in opportunities:
            if Config.is_leveraged_token(opp.base_asset):
                self.logger.debug(f"Analyzer: Filtering out leveraged token opportunity: {opp.get_unique_id()}")
                continue
            
            if (opp.buy_exchange_id.lower(), opp.base_asset.upper()) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
               (opp.sell_exchange_id.lower(), opp.base_asset.upper()) in Config.ASSET_UNAVAILABLE_BLACKLIST:
                self.logger.debug(f"Analyzer: Filtering out opportunity {opp.get_unique_id()} due to general ASSET_UNAVAILABLE_BLACKLIST.")
                continue

            valid_opportunities_pre_stability.append(opp)

        if not valid_opportunities_pre_stability:
            self.logger.info("Analyzer: All provided opportunities were leveraged or generally blacklisted. No valid opportunities to analyze.")
            return None

        self._update_stability_counts(valid_opportunities_pre_stability)

        stable_opportunities_gross = [
            opp for opp in self.stable_candidates.values()
            if opp.is_stable
        ]

        if not stable_opportunities_gross:
            self.logger.info("Analyzer: No opportunities met the STABILITY_CYCLES requirement.")
            return None
        
        self.logger.info(f"Analyzer: Found {len(stable_opportunities_gross)} stable gross opportunities. Enriching top {Config.TOP_N_OPPORTUNITIES} by gross profit...")
        stable_opportunities_gross.sort(key=lambda x: x.gross_profit_pct, reverse=True)
        
        enriched_stable_opportunities = await self._enrich_opportunities_with_fees(
            stable_opportunities_gross[:Config.TOP_N_OPPORTUNITIES]
        )

        final_candidate_opportunities: List[ArbitrageOpportunity] = []
        for opp in enriched_stable_opportunities:
            if not opp.potential_networks:
                self.logger.debug(f"Opp {opp.get_unique_id()} has no potential_networks after enrichment. Skipping whitelist/blacklist check.")
                continue

            selected_network_for_this_opp = False
            for net_detail in opp.potential_networks:
                current_path_tuple = (
                    opp.base_asset.upper(),
                    opp.buy_exchange_id.lower(),
                    opp.sell_exchange_id.lower(),
                    net_detail['normalized_name'].upper()
                )

                if current_path_tuple in Config.ARB_PATH_BLACKLIST:
                    self.logger.debug(f"Path {current_path_tuple} for opp {opp.get_unique_id()} is specifically blacklisted. Checking next network option for this opp.")
                    continue

                is_this_network_whitelisted = current_path_tuple in Config.ARB_WHITELIST
                
                if Config.ENFORCE_WHITELIST_ONLY and not is_this_network_whitelisted:
                    self.logger.debug(f"Path {current_path_tuple} for opp {opp.get_unique_id()} is not whitelisted (enforcement on). Checking next network option.")
                    continue
                
                initial_best_net_detail_in_enrich = opp.chosen_network_details
                
                if net_detail['normalized_name'] != (initial_best_net_detail_in_enrich.get('normalized_name') if initial_best_net_detail_in_enrich else None):
                    self.logger.debug(f"Re-evaluating net profit for {opp.get_unique_id()} with network {net_detail['normalized_name']} (was previously {initial_best_net_detail_in_enrich.get('normalized_name') if initial_best_net_detail_in_enrich else 'None'}) due to whitelist/blacklist selection.")
                    new_withdrawal_fee_usd = None
                    native_fee = net_detail['fee_native']
                    fee_currency = net_detail.get('fee_currency', opp.base_asset)

                    if fee_currency.upper() == opp.base_asset.upper():
                        new_withdrawal_fee_usd = native_fee * opp.buy_price
                    elif fee_currency.upper() == Config.QUOTE_ASSET.upper():
                        new_withdrawal_fee_usd = native_fee
                    else:
                        fee_curr_price_usdt = await self.get_asset_price_in_usdt(fee_currency, preferred_ref_exchange_id=opp.buy_exchange_id, opportunity_context=opp)
                        if fee_curr_price_usdt and fee_curr_price_usdt > 0:
                            new_withdrawal_fee_usd = native_fee * fee_curr_price_usdt
                        else:
                            self.logger.warning(f"Could not get price for fee currency {fee_currency} for {opp.get_unique_id()}. Using default high fee.")
                            new_withdrawal_fee_usd = Config.ESTIMATED_WITHDRAWAL_FEES_USD.get('DEFAULT_ASSET_FEE')
                    
                    opp.withdrawal_fee_usd = new_withdrawal_fee_usd
                    if new_withdrawal_fee_usd is not None and Config.TRADE_AMOUNT_USD > 0:
                        withdrawal_fee_pct_of_trade = (new_withdrawal_fee_usd / Config.TRADE_AMOUNT_USD) * Decimal('100')
                        opp.net_profit_pct = opp.gross_profit_pct - (opp.buy_fee_pct or 0) - (opp.sell_fee_pct or 0) - withdrawal_fee_pct_of_trade
                    else:
                        opp.net_profit_pct = None
                
                opp.chosen_network = net_detail['normalized_name']
                opp.chosen_network_details = net_detail

                final_candidate_opportunities.append(opp)
                selected_network_for_this_opp = True
                break

            if not selected_network_for_this_opp:
                 self.logger.debug(f"Opp {opp.get_unique_id()} had no valid (whitelisted/not_blacklisted) networks in its potential_networks list after filtering. Skipping this opp.")
        
        if not final_candidate_opportunities:
            self.logger.info("Analyzer: No opportunities remained after whitelist/blacklist filtering.")
            return None

        net_profitable_opportunities = [
            opp for opp in final_candidate_opportunities
            if opp.net_profit_pct is not None and opp.net_profit_pct >= Config.MIN_PROFIT_THRESHOLD_NET
        ]

        if not net_profitable_opportunities:
            self.logger.info("Analyzer: No opportunities remained net profitable after whitelist/blacklist and fee calculation.")
            return None

        net_profitable_opportunities.sort(key=lambda x: x.net_profit_pct, reverse=True)

        best_opportunity_final: Optional[ArbitrageOpportunity] = None
        for potential_best_opp in net_profitable_opportunities:
            if not potential_best_opp.chosen_network_details:
                 self.logger.warning(f"Opportunity {potential_best_opp.get_unique_id()} has no chosen_network_details. Skipping liquidity check.")
                 continue

            if self.executor and hasattr(self.executor, '_check_liquidity_for_trade_leg'):
                self.logger.debug(f"Performing final liquidity check for selected opportunity: {potential_best_opp.get_unique_id()}")
                
                buy_liquid = await self.executor._check_liquidity_for_trade_leg(
                    potential_best_opp.buy_exchange_id, potential_best_opp.symbol, 'buy',
                    Config.TRADE_AMOUNT_USD, potential_best_opp.buy_price, is_base_amount=False
                )
                
                approx_base_amount = Config.TRADE_AMOUNT_USD / potential_best_opp.buy_price if potential_best_opp.buy_price > 0 else Decimal(0)
                sell_liquid = False
                if approx_base_amount > 0:
                    sell_liquid = await self.executor._check_liquidity_for_trade_leg(
                        potential_best_opp.sell_exchange_id, potential_best_opp.symbol, 'sell',
                        approx_base_amount, potential_best_opp.sell_price, is_base_amount=True
                    )
                
                potential_best_opp.is_liquid_enough_for_trade = buy_liquid and sell_liquid
                if potential_best_opp.is_liquid_enough_for_trade:
                    best_opportunity_final = potential_best_opp
                    break
                else:
                    self.logger.warning(f"Opportunity {potential_best_opp.get_unique_id()} failed final liquidity check. Buy liquid: {buy_liquid}, Sell liquid: {sell_liquid}. Trying next.")
            else:
                self.logger.warning("Executor or _check_liquidity_for_trade_leg not available. Selecting best opportunity without final liquidity check.")
                potential_best_opp.is_liquid_enough_for_trade = True
                best_opportunity_final = potential_best_opp
                break
        
        if not best_opportunity_final:
            self.logger.info("Analyzer: No net profitable opportunities passed the final liquidity check (if performed).")
            return None

        self.logger.info(f"🏆 Best Full Arbitrage Opportunity Selected: {best_opportunity_final.get_unique_id()} | Symbol: {best_opportunity_final.symbol} | "
                         f"Gross Profit: {best_opportunity_final.gross_profit_pct:.3f}% -> Net Profit: {best_opportunity_final.net_profit_pct:.3f}% "
                         f"(Buy: {best_opportunity_final.buy_exchange_id} @ {best_opportunity_final.buy_price:.8f}, Sell: {best_opportunity_final.sell_exchange_id} @ {best_opportunity_final.sell_price:.8f}) "
                         f"Withdrawal Fee (est.): ${best_opportunity_final.withdrawal_fee_usd if best_opportunity_final.withdrawal_fee_usd is not None else 'N/A':.2f} via {best_opportunity_final.chosen_network or 'N/A'} "
                         f"Liquid: {best_opportunity_final.is_liquid_enough_for_trade}")

        if best_opportunity_final.get_unique_id() in self.stable_candidates:
            del self.stable_candidates[best_opportunity_final.get_unique_id()]
            self.logger.debug(f"Removed chosen opportunity {best_opportunity_final.get_unique_id()} from stable_candidates.")

        return best_opportunity_final

    def _update_stability_counts(self, current_scan_opportunities: List[ArbitrageOpportunity]):
        current_opportunity_ids_set = {opp.get_unique_id() for opp in current_scan_opportunities}

        for opp in current_scan_opportunities:
            uid = opp.get_unique_id()
            if uid in self.stable_candidates:
                self.stable_candidates[uid].stability_count += 1
                self.stable_candidates[uid].buy_price = opp.buy_price
                self.stable_candidates[uid].sell_price = opp.sell_price
                self.stable_candidates[uid].gross_profit_pct = opp.gross_profit_pct
            else:
                self.stable_candidates[uid] = opp
                self.stable_candidates[uid].stability_count = 1

            if self.stable_candidates[uid].stability_count >= Config.STABILITY_CYCLES:
                self.stable_candidates[uid].is_stable = True
        
        stale_candidate_uids = [uid for uid in self.stable_candidates if uid not in current_opportunity_ids_set]
        for uid_to_remove in stale_candidate_uids:
            removed_candidate = self.stable_candidates.pop(uid_to_remove, None)
            if removed_candidate:
                 self.logger.debug(f"Removed stale/disappeared candidate: {removed_candidate.get_unique_id()} (was stable: {removed_candidate.is_stable}, count: {removed_candidate.stability_count})")
        if stale_candidate_uids:
            self.logger.debug(f"Removed {len(stale_candidate_uids)} stale/disappeared candidates from stability tracking.")

    @safe_call_wrapper(default_retval=[])
    async def _enrich_opportunities_with_fees(self, opportunities: List[ArbitrageOpportunity]) -> List[ArbitrageOpportunity]:
        self.logger.debug(f"Enriching {len(opportunities)} opportunities with detailed fee data (including base asset withdrawal)...")
        enriched_list: List[ArbitrageOpportunity] = []

        await self._cache_all_currencies_info_for_enrichment(opportunities)

        for opp in opportunities:
            self.logger.debug(f"Enriching details for opportunity: {opp.get_unique_id()}")
            try:
                buy_ex_inst = self.exchanges.get(opp.buy_exchange_id)
                sell_ex_inst = self.exchanges.get(opp.sell_exchange_id)

                if not buy_ex_inst or not sell_ex_inst or \
                   not buy_ex_inst.markets or opp.symbol not in buy_ex_inst.markets or \
                   not sell_ex_inst.markets or opp.symbol not in sell_ex_inst.markets:
                    self.logger.warning(f"Market data or exchange instance missing for {opp.get_unique_id()}. Cannot enrich. Skipping.")
                    opp.net_profit_pct = None
                    enriched_list.append(opp)
                    continue
                
                buy_market_details = buy_ex_inst.markets[opp.symbol]
                sell_market_details = sell_ex_inst.markets[opp.symbol]

                default_taker_fee_rate = Decimal('0.001')

                buy_taker_fee_rate_str = buy_market_details.get('taker', default_taker_fee_rate)
                sell_taker_fee_rate_str = sell_market_details.get('taker', default_taker_fee_rate)

                try:
                    opp.buy_fee_pct = Decimal(str(buy_taker_fee_rate_str)) * Decimal('100')
                    opp.sell_fee_pct = Decimal(str(sell_taker_fee_rate_str)) * Decimal('100')
                except (InvalidOperation, TypeError) as e_fee_parse:
                    self.logger.warning(f"Could not parse taker fee for {opp.get_unique_id()}: {e_fee_parse}. Using default 0.1%. Buy raw: '{buy_taker_fee_rate_str}', Sell raw: '{sell_taker_fee_rate_str}'")
                    opp.buy_fee_pct = default_taker_fee_rate * Decimal('100')
                    opp.sell_fee_pct = default_taker_fee_rate * Decimal('100')

                approx_base_to_withdraw = Config.TRADE_AMOUNT_USD / opp.buy_price if opp.buy_price > 0 else Decimal(0)
                
                potential_networks_list = await self._select_optimal_network_for_transfer(
                    asset_code=opp.base_asset,
                    from_exchange_id=opp.buy_exchange_id,
                    to_exchange_id=opp.sell_exchange_id,
                    amount_native_to_withdraw_optional=approx_base_to_withdraw
                )
                opp.potential_networks = potential_networks_list if potential_networks_list else []

                opp.withdrawal_fee_usd = None
                opp.chosen_network = None
                opp.chosen_network_details = None

                if opp.potential_networks:
                    best_network_for_fee_calc = opp.potential_networks[0]
                    opp.chosen_network = best_network_for_fee_calc['normalized_name']
                    opp.chosen_network_details = best_network_for_fee_calc

                    native_fee = best_network_for_fee_calc['fee_native']
                    fee_currency = best_network_for_fee_calc.get('fee_currency', opp.base_asset)

                    if fee_currency.upper() == opp.base_asset.upper():
                        opp.withdrawal_fee_usd = native_fee * opp.buy_price
                    elif fee_currency.upper() == Config.QUOTE_ASSET.upper():
                        opp.withdrawal_fee_usd = native_fee
                    else:
                        fee_curr_price_usdt = await self.get_asset_price_in_usdt(fee_currency, preferred_ref_exchange_id=opp.buy_exchange_id, opportunity_context=opp)
                        if fee_curr_price_usdt and fee_curr_price_usdt > 0:
                            opp.withdrawal_fee_usd = native_fee * fee_curr_price_usdt
                        else:
                            self.logger.warning(f"Could not get price for fee currency {fee_currency} to calculate withdrawal fee in USD for {opp.get_unique_id()}. Estimating high.")
                            opp.withdrawal_fee_usd = Config.ESTIMATED_WITHDRAWAL_FEES_USD.get('DEFAULT_ASSET_FEE')
                else:
                    self.logger.warning(f"No suitable networks found for withdrawing {opp.base_asset} from {opp.buy_exchange_id} to {opp.sell_exchange_id}. Using default high withdrawal fee estimate.")
                    asset_default_fees = Config.ESTIMATED_WITHDRAWAL_FEES_USD.get(opp.base_asset.upper(), {})
                    opp.withdrawal_fee_usd = asset_default_fees.get('DEFAULT', Config.ESTIMATED_WITHDRAWAL_FEES_USD['DEFAULT_ASSET_FEE'])

                if opp.withdrawal_fee_usd is not None and Config.TRADE_AMOUNT_USD > 0:
                    withdrawal_fee_pct_of_trade = (opp.withdrawal_fee_usd / Config.TRADE_AMOUNT_USD) * Decimal('100')
                    
                    opp.net_profit_pct = opp.gross_profit_pct - \
                                         (opp.buy_fee_pct or 0) - \
                                         (opp.sell_fee_pct or 0) - \
                                         withdrawal_fee_pct_of_trade
                    
                    self.logger.info(f"Enriched (Full Arbitrage): {opp.get_unique_id()} | "
                                     f"Gross: {opp.gross_profit_pct:.3f}%, BuyFee: {opp.buy_fee_pct or 0:.3f}%, SellFee: {opp.sell_fee_pct or 0:.3f}%, "
                                     f"WithdrawFee ({opp.base_asset} via {opp.chosen_network or 'N/A'}): ~${opp.withdrawal_fee_usd:.2f} ({withdrawal_fee_pct_of_trade:.3f}%) "
                                     f"-> Net Profit Pct: {opp.net_profit_pct:.3f}%")
                else:
                    opp.net_profit_pct = None
                    self.logger.warning(f"Net profit not calculated for {opp.get_unique_id()} due to missing withdrawal fee calculation or zero trade amount.")
                
                enriched_list.append(opp)

            except Exception as e_enrich:
                self.logger.error(f"Error enriching opportunity {opp.get_unique_id()}: {e_enrich}", exc_info=True)
                opp.net_profit_pct = None
                enriched_list.append(opp)
        return enriched_list

    @safe_call_wrapper(default_retval=None)
    async def _cache_all_currencies_info_for_enrichment(self, opportunities: List[ArbitrageOpportunity]):
        exchanges_needing_currency_fetch = set()
        for ex_id in self.exchanges.keys():
             if ex_id not in self.currencies_cache or not self.currencies_cache.get(ex_id):
                exchanges_needing_currency_fetch.add(ex_id)

        for opp in opportunities:
            if opp.buy_exchange_id not in self.currencies_cache and opp.buy_exchange_id in self.exchanges:
                exchanges_needing_currency_fetch.add(opp.buy_exchange_id)
            if opp.sell_exchange_id not in self.currencies_cache and opp.sell_exchange_id in self.exchanges:
                 exchanges_needing_currency_fetch.add(opp.sell_exchange_id)


        if not exchanges_needing_currency_fetch:
            self.logger.debug("All required currency information for enrichment/network selection appears to be cached.")
            return

        self.logger.info(f"Fetching/refreshing currency information from CCXT for exchanges: {list(exchanges_needing_currency_fetch)}")
        currency_fetch_tasks: Dict[str, asyncio.Task] = {}
        original_timeouts_curr = {}

        for ex_id in exchanges_needing_currency_fetch:
            ex_inst = self.exchanges.get(ex_id)
            if ex_inst and hasattr(ex_inst, 'has') and ex_inst.has.get('fetchCurrencies'):
                original_timeouts_curr[ex_id] = getattr(ex_inst, 'timeout', 30000)
                ex_inst.timeout = 90000
                self.logger.debug(f"Fetching currencies for {ex_id} with timeout {ex_inst.timeout}ms")
                currency_fetch_tasks[ex_id] = asyncio.create_task(ex_inst.fetch_currencies())
            else:
                self.logger.warning(f"Exchange {ex_id} does not support fetchCurrencies or instance not available. Currency cache for it will be empty or unchanged.")
                self.currencies_cache[ex_id] = self.currencies_cache.get(ex_id, {})

        if currency_fetch_tasks:
            task_keys_ordered = list(currency_fetch_tasks.keys())
            results = await asyncio.gather(*currency_fetch_tasks.values(), return_exceptions=True)
            for ex_id, result in zip(task_keys_ordered, results):
                ex_inst_after_fetch = self.exchanges.get(ex_id)
                if ex_inst_after_fetch and ex_id in original_timeouts_curr:
                    setattr(ex_inst_after_fetch, 'timeout', original_timeouts_curr[ex_id])

                if not isinstance(result, Exception) and result and isinstance(result, dict):
                    self.currencies_cache[ex_id] = result
                    self.logger.info(f"Successfully cached/updated currency information for {ex_id} (Total currencies: {len(result)}).")
                elif isinstance(result, Exception):
                    self.logger.error(f"Failed to fetch currencies for {ex_id}: {result}. Existing cache (if any) retained.")
                    self.currencies_cache[ex_id] = self.currencies_cache.get(ex_id, {})
                else:
                    self.logger.warning(f"No currency data returned or unexpected format from {ex_id}. Result type: {type(result)}. Using existing/empty cache.")
                    self.currencies_cache[ex_id] = self.currencies_cache.get(ex_id, {})
        self.logger.debug("Finished caching/refreshing currency information from CCXT.")

    @safe_call_wrapper(default_retval=[])
    async def _select_optimal_network_for_transfer(self,
                                               asset_code: str,
                                               from_exchange_id: str,
                                               to_exchange_id: str,
                                               amount_native_to_withdraw_optional: Optional[Decimal] = None
                                               ) -> List[Dict[str, Any]]:
        self.logger.debug(f"Selecting optimal transfer network(s) for {asset_code} from {from_exchange_id} to {to_exchange_id}"
                          f"{f' (amount: {amount_native_to_withdraw_optional})' if amount_native_to_withdraw_optional else ''}")
        asset_code_upper = asset_code.upper()

        # --- ИСПРАВЛЕНО: Применение нового правила TOKEN_NETWORK_RESTRICTIONS ---
        restriction_key = (from_exchange_id.lower(), asset_code_upper)
        allowed_networks_for_path = Config.TOKEN_NETWORK_RESTRICTIONS.get(restriction_key)
        if allowed_networks_for_path:
            self.logger.info(f"Applying network restriction for {asset_code_upper} from {from_exchange_id}: Only networks {allowed_networks_for_path} are allowed.")

        if (from_exchange_id.lower(), asset_code_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
           (to_exchange_id.lower(), asset_code_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST:
            self.logger.warning(f"Asset {asset_code_upper} transfer involving {from_exchange_id} or {to_exchange_id} is generally blacklisted. No networks will be selected.")
            return []

        potential_from_networks_details: List[Dict[str, Any]] = []
        
        from_ex_id_lower = from_exchange_id.lower()
        from_asset_direct_data = Config.LOADED_EXCHANGE_FEES_DATA.get(from_ex_id_lower, {}).get(asset_code_upper, {})
        from_networks_direct = from_asset_direct_data.get('networks', {}) if isinstance(from_asset_direct_data, dict) else {}

        for norm_net_name_direct, direct_net_info in from_networks_direct.items():
            if not isinstance(direct_net_info, dict) or direct_net_info.get('fee') is None:
                continue
            if not direct_net_info.get('can_withdraw', True):
                self.logger.debug(f"JSON Network (FROM): '{direct_net_info.get('original_name_from_file', norm_net_name_direct)}' on {from_exchange_id} for {asset_code_upper} is marked 'can_withdraw: false'. Skipping.")
                continue

            # --- ИСПРАВЛЕНО: Проверка на соответствие правилу TOKEN_NETWORK_RESTRICTIONS ---
            if allowed_networks_for_path and norm_net_name_direct not in allowed_networks_for_path:
                self.logger.debug(f"Skipping network '{norm_net_name_direct}' from direct config for {asset_code_upper} from {from_exchange_id} due to restriction. Allowed: {allowed_networks_for_path}")
                continue

            original_name_from_direct_data = direct_net_info.get('original_name_from_file', norm_net_name_direct)
            min_wd_val = direct_net_info.get('min_withdraw', Decimal('0'))
            try:
                fee_dec = Decimal(str(direct_net_info['fee']))
                min_wd_dec = Decimal(str(min_wd_val)) if min_wd_val is not None else Decimal('0')
            except (InvalidOperation, TypeError) as e_parse:
                self.logger.warning(f"Error parsing fee/min_withdrawal for direct_data network '{original_name_from_direct_data}' (Asset: {asset_code_upper}, Ex: {from_exchange_id}): {e_parse}. Skipping.")
                continue

            potential_from_networks_details.append({
                'withdraw_network_code_on_from_ex': original_name_from_direct_data,
                'normalized_name': norm_net_name_direct,
                'fee_native': fee_dec,
                'fee_currency': asset_code_upper,
                'min_withdrawal_native': min_wd_dec,
                'source_of_fee': "DIRECT_CONFIG_DATA",
                'can_withdraw': True
            })
            self.logger.debug(f"Added network from DIRECT_CONFIG_DATA (FROM): '{original_name_from_direct_data}' (Normalized: {norm_net_name_direct}) for {asset_code_upper} on {from_exchange_id}.")

        if from_exchange_id not in self.currencies_cache or not self.currencies_cache.get(from_exchange_id):
            await self._cache_all_currencies_info_for_enrichment([])

        from_ex_ccxt_currencies = self.currencies_cache.get(from_exchange_id, {})
        from_asset_ccxt_details = from_ex_ccxt_currencies.get(asset_code_upper, {}) if isinstance(from_ex_ccxt_currencies, dict) else {}
        from_networks_ccxt = from_asset_ccxt_details.get('networks', {}) if isinstance(from_asset_ccxt_details, dict) else {}
        
        for ccxt_net_original_name, ccxt_net_info in from_networks_ccxt.items():
            if not isinstance(ccxt_net_info, dict): continue
            
            normalized_ccxt_net_name = Config.normalize_network_name_for_config(ccxt_net_original_name)
            
            # --- ИСПРАВЛЕНО: Проверка на соответствие правилу TOKEN_NETWORK_RESTRICTIONS ---
            if allowed_networks_for_path and normalized_ccxt_net_name not in allowed_networks_for_path:
                self.logger.debug(f"Skipping network '{normalized_ccxt_net_name}' from CCXT for {asset_code_upper} from {from_exchange_id} due to restriction. Allowed: {allowed_networks_for_path}")
                continue

            if any(pfn['normalized_name'] == normalized_ccxt_net_name for pfn in potential_from_networks_details):
                self.logger.debug(f"Network {normalized_ccxt_net_name} (from CCXT original: {ccxt_net_original_name}) on {from_exchange_id} already covered by direct config data. Skipping CCXT version for FROM_EX.")
                continue

            is_ccxt_net_active = ccxt_net_info.get('active', True)
            is_ccxt_net_withdrawable = ccxt_net_info.get('withdraw', True)
            if not (is_ccxt_net_active and is_ccxt_net_withdrawable):
                self.logger.debug(f"CCXT Network (FROM): '{ccxt_net_original_name}' on {from_exchange_id} for {asset_code_upper} is inactive or not withdrawable. Skipping.")
                continue

            fee_native_val = ccxt_net_info.get('fee')
            fee_currency_code = ccxt_net_info.get('feeCurrency', asset_code_upper)
            min_withdrawal_native_val = ccxt_net_info.get('limits', {}).get('withdraw', {}).get('min')
            
            if fee_native_val is None:
                self.logger.debug(f"CCXT Network (FROM): '{ccxt_net_original_name}' on {from_exchange_id} for {asset_code_upper} has no fee information. Skipping.")
                continue
            try:
                fee_decimal = Decimal(str(fee_native_val))
                min_withdrawal_decimal = Decimal(str(min_withdrawal_native_val)) if min_withdrawal_native_val is not None else Decimal(0)
            except (InvalidOperation, TypeError) as e_parse:
                self.logger.warning(f"Error parsing fee/min_withdrawal for CCXT network '{ccxt_net_original_name}' (Asset: {asset_code_upper}, Ex: {from_exchange_id}): {e_parse}. Skipping.")
                continue
            
            potential_from_networks_details.append({
                'withdraw_network_code_on_from_ex': ccxt_net_original_name,
                'normalized_name': normalized_ccxt_net_name,
                'fee_native': fee_decimal,
                'fee_currency': fee_currency_code.upper(),
                'min_withdrawal_native': min_withdrawal_decimal,
                'source_of_fee': "CCXT_API_DATA",
                'can_withdraw': True
            })
            self.logger.debug(f"Added network from CCXT_API_DATA (FROM): '{ccxt_net_original_name}' (Normalized: {normalized_ccxt_net_name}) for {asset_code_upper} on {from_exchange_id}.")
        
        self.logger.debug(f"[{from_exchange_id}->{to_exchange_id}|{asset_code_upper}] Found {len(potential_from_networks_details)} potential WITHDRAWAL networks on {from_exchange_id}: "
                          f"{[d['normalized_name'] for d in potential_from_networks_details]}")

        if not potential_from_networks_details:
            self.logger.warning(f"No networks found for withdrawing {asset_code_upper} from {from_exchange_id} (or all are disabled/blacklisted). Cannot select optimal network."); return []

        active_deposit_normalized_nets_on_to_ex: Dict[str, str] = {}
        
        to_ex_id_lower = to_exchange_id.lower()
        to_asset_direct_data = Config.LOADED_EXCHANGE_FEES_DATA.get(to_ex_id_lower, {}).get(asset_code_upper, {})
        to_networks_direct_deposit = to_asset_direct_data.get('networks', {}) if isinstance(to_asset_direct_data, dict) else {}

        for norm_net_name_direct_to, direct_to_info in to_networks_direct_deposit.items():
            if isinstance(direct_to_info, dict) and direct_to_info.get('can_deposit', True):
                if norm_net_name_direct_to not in active_deposit_normalized_nets_on_to_ex:
                    active_deposit_normalized_nets_on_to_ex[norm_net_name_direct_to] = direct_to_info.get('original_name_from_file', norm_net_name_direct_to)
                    self.logger.debug(f"Found deposit-enabled network from DIRECT_CONFIG_DATA (TO): '{direct_to_info.get('original_name_from_file', norm_net_name_direct_to)}' (Normalized: {norm_net_name_direct_to}) for {asset_code_upper} on {to_exchange_id}.")
            elif isinstance(direct_to_info, dict) and not direct_to_info.get('can_deposit', True):
                 self.logger.debug(f"JSON Network (TO): '{direct_to_info.get('original_name_from_file', norm_net_name_direct_to)}' on {to_exchange_id} for {asset_code_upper} is marked 'can_deposit: false'. Skipping.")

        if to_exchange_id not in self.currencies_cache or not self.currencies_cache.get(to_exchange_id):
            await self._cache_all_currencies_info_for_enrichment([])

        to_ex_ccxt_currencies = self.currencies_cache.get(to_exchange_id, {})
        to_asset_ccxt_details = to_ex_ccxt_currencies.get(asset_code_upper, {}) if isinstance(to_ex_ccxt_currencies, dict) else {}
        to_networks_ccxt = to_asset_ccxt_details.get('networks', {}) if isinstance(to_asset_ccxt_details, dict) else {}

        for to_net_orig_ccxt, to_net_info_ccxt in to_networks_ccxt.items():
            if isinstance(to_net_info_ccxt, dict) and \
               to_net_info_ccxt.get('active', True) and \
               to_net_info_ccxt.get('deposit', True):
                norm_name_to = Config.normalize_network_name_for_config(to_net_orig_ccxt)
                if norm_name_to not in active_deposit_normalized_nets_on_to_ex:
                    active_deposit_normalized_nets_on_to_ex[norm_name_to] = to_net_orig_ccxt
                    self.logger.debug(f"Found deposit-enabled network from CCXT_API_DATA (TO): '{to_net_orig_ccxt}' (Normalized: {norm_name_to}) for {asset_code_upper} on {to_exchange_id}.")
            elif isinstance(to_net_info_ccxt, dict) and not (to_net_info_ccxt.get('active', True) and to_net_info_ccxt.get('deposit', True)):
                 self.logger.debug(f"CCXT Network (TO): '{to_net_orig_ccxt}' on {to_exchange_id} for {asset_code_upper} is inactive or not depositable. Skipping.")


        self.logger.debug(f"[{from_exchange_id}->{to_exchange_id}|{asset_code_upper}] Found {len(active_deposit_normalized_nets_on_to_ex)} active DEPOSIT networks on {to_exchange_id}: "
                          f"{list(active_deposit_normalized_nets_on_to_ex.keys())}")

        if not active_deposit_normalized_nets_on_to_ex:
            self.logger.warning(f"No active networks found for depositing {asset_code_upper} to {to_exchange_id}. Cannot find transfer path.")
            return []
        
        final_candidate_networks: List[Dict[str, Any]] = []
        for from_net_detail in potential_from_networks_details:
            normalized_name_to_match = from_net_detail['normalized_name']

            if normalized_name_to_match in active_deposit_normalized_nets_on_to_ex and \
               normalized_name_to_match != 'UNKNOWN_NETWORK':

                if amount_native_to_withdraw_optional is not None and \
                   from_net_detail['min_withdrawal_native'] > 0 and \
                   amount_native_to_withdraw_optional < from_net_detail['min_withdrawal_native']:
                    self.logger.debug(f"  Skipping network {normalized_name_to_match} for {asset_code_upper} transfer: "
                                      f"Amount {amount_native_to_withdraw_optional} < Min Withdrawal {from_net_detail['min_withdrawal_native']}")
                    continue

                deposit_net_code_on_to_ex = active_deposit_normalized_nets_on_to_ex.get(normalized_name_to_match)
                if not deposit_net_code_on_to_ex:
                    self.logger.warning(f"Internal inconsistency: Could not find API deposit code for normalized network '{normalized_name_to_match}' on {to_exchange_id}. Skipping this path for safety.")
                    continue
                
                final_candidate_networks.append({
                    'withdraw_network_code_on_from_ex': from_net_detail['withdraw_network_code_on_from_ex'],
                    'deposit_network_code_on_to_ex': deposit_net_code_on_to_ex,
                    'normalized_name': normalized_name_to_match,
                    'fee_native': from_net_detail['fee_native'],
                    'fee_currency': from_net_detail['fee_currency'],
                    'min_withdrawal_native': from_net_detail['min_withdrawal_native'],
                    'source_of_fee': from_net_detail['source_of_fee'],
                    'priority_score_token': Config.get_network_priority_score(normalized_name_to_match, asset_code_upper),
                    'priority_score_general': Config.get_network_priority_score(normalized_name_to_match)
                })
                self.logger.debug(f"  Found compatible and ENABLED transfer path for {asset_code_upper}: "
                                  f"From Ex: {from_exchange_id} (Withdraw Net Code: {from_net_detail['withdraw_network_code_on_from_ex']}) -> "
                                  f"To Ex: {to_exchange_id} (Deposit Net Code for fetchDepositAddress: {deposit_net_code_on_to_ex}). "
                                  f"Normalized: {normalized_name_to_match}, Fee: {from_net_detail['fee_native']} {from_net_detail['fee_currency']}")

        if not final_candidate_networks:
            self.logger.warning(f"No common, active, and enabled (withdraw & deposit) networks found for transferring {asset_code_upper} from {from_exchange_id} to {to_exchange_id} after detailed check, or amount too low for all.")
            return []

        unique_fee_currencies = set(net.get('fee_currency', asset_code_upper) for net in final_candidate_networks)
        fee_currency_prices_in_usd: Dict[str, Optional[Decimal]] = {}
        for fee_curr_code in unique_fee_currencies:
            price_in_usdt = await self.get_asset_price_in_usdt(fee_curr_code, preferred_ref_exchange_id=from_exchange_id)
            if price_in_usdt and price_in_usdt > 0:
                 fee_currency_prices_in_usd[fee_curr_code.upper()] = price_in_usdt
            else:
                estimated_fee_curr_price = Config.ESTIMATED_ASSET_PRICES_USD.get(fee_curr_code.upper())
                if estimated_fee_curr_price:
                    fee_currency_prices_in_usd[fee_curr_code.upper()] = estimated_fee_curr_price
                    self.logger.debug(f"Used ESTIMATED price for fee currency {fee_curr_code}: ${estimated_fee_curr_price}")
                else:
                     self.logger.warning(f"Could not determine USD price for fee currency '{fee_curr_code}'. Withdrawal fee sorting for this currency might be inaccurate.")
                     fee_currency_prices_in_usd[fee_curr_code.upper()] = None

        def sort_key_for_networks(network_option: Dict[str, Any]) -> Tuple[Decimal, int, int]:
            fee_native = network_option['fee_native']
            fee_curr_upper = network_option.get('fee_currency', asset_code_upper).upper()
            fee_in_usd_equivalent = Decimal('Infinity')

            price_of_fee_asset_in_usd = fee_currency_prices_in_usd.get(fee_curr_upper)

            if price_of_fee_asset_in_usd and price_of_fee_asset_in_usd > 0:
                fee_in_usd_equivalent = fee_native * price_of_fee_asset_in_usd
            elif fee_curr_upper == Config.QUOTE_ASSET.upper():
                 fee_in_usd_equivalent = fee_native
            else:
                self.logger.debug(f"Network '{network_option['normalized_name']}' (fee currency '{fee_curr_upper}') - USD price for fee currency unknown. Sorting with high effective fee.")

            token_pref_score = network_option['priority_score_token']
            general_pref_score = network_option['priority_score_general']
            
            return (fee_in_usd_equivalent, token_pref_score, general_pref_score)

        final_candidate_networks.sort(key=sort_key_for_networks)

        if not final_candidate_networks:
            self.logger.warning(f"No candidate networks remained after sorting for {asset_code_upper} from {from_exchange_id} to {to_exchange_id}.")
            return []

        num_to_log = min(3, len(final_candidate_networks))
        self.logger.info(f"Top {num_to_log} potential networks for {asset_code_upper} ({from_exchange_id} -> {to_exchange_id}) after sorting by fee/preference:")
        for i, net_opt in enumerate(final_candidate_networks[:num_to_log]):
            fee_usd_log_detail = ""
            fee_price = fee_currency_prices_in_usd.get(net_opt['fee_currency'].upper())
            if fee_price and fee_price > 0:
                fee_usd_log_detail = f" (~${(net_opt['fee_native'] * fee_price):.2f} USD)"
            self.logger.info(f"  {i+1}. Normalized: '{net_opt['normalized_name']}', "
                             f"Withdraw Code (on {from_exchange_id}): '{net_opt['withdraw_network_code_on_from_ex']}', "
                             f"Deposit Code (on {to_exchange_id}): '{net_opt['deposit_network_code_on_to_ex']}', "
                             f"Fee: {net_opt['fee_native']} {net_opt.get('fee_currency', asset_code_upper)}{fee_usd_log_detail}, "
                             f"MinW: {net_opt['min_withdrawal_native']} {asset_code_upper}, "
                             f"Scores (Tok/Gen): {net_opt['priority_score_token']}/{net_opt['priority_score_general']}")

        return final_candidate_networks

    async def _check_orderbook_liquidity(self, exchange_id: str, symbol: str, side: str,
                                         amount_base: Decimal, price_target: Decimal,
                                         depth_pct_allowed_slippage: Decimal = Decimal("1.0")
                                         ) -> bool:
        self.logger.debug(f"Liquidity check for {exchange_id} - {symbol} ({side}) "
                          f"Amount: {amount_base:.8f} Base, Price Target: {price_target:.8f}. "
                          f"Slippage allowance: {depth_pct_allowed_slippage}%, Min Market Liquidity: ${Config.MIN_LIQUIDITY_USD}")

        exchange = self.exchanges.get(exchange_id)
        if not exchange or not exchange.has.get('fetchOrderBook'):
            self.logger.warning(f"Exchange {exchange_id} does not support fetchOrderBook or not found. Assuming liquid for {symbol}.")
            return True

        try:
            orderbook = await exchange.fetch_order_book(symbol, limit=20)
            relevant_orders = orderbook['asks' if side == 'buy' else 'bids']

            if not relevant_orders:
                self.logger.warning(f"No {'asks' if side == 'buy' else 'bids'} found in orderbook for {symbol} on {exchange_id}. Assuming not liquid enough.")
                return False

            total_visible_liquidity_usd = sum(Decimal(str(p)) * Decimal(str(a)) for p, a in relevant_orders)
            if total_visible_liquidity_usd < Config.MIN_LIQUIDITY_USD:
                self.logger.warning(
                    f"Liquidity check FAIL (Market Depth): Total visible liquidity for {symbol} on {exchange_id} "
                    f"is only ~${total_visible_liquidity_usd:.2f}, which is below the required minimum of ${Config.MIN_LIQUIDITY_USD}."
                )
                return False
            
            accumulated_base_amount = Decimal('0')
            accumulated_quote_cost = Decimal('0')
            price_limit_buy = price_target * (Decimal('1') + (depth_pct_allowed_slippage / Decimal('100')))
            price_limit_sell = price_target * (Decimal('1') - (depth_pct_allowed_slippage / Decimal('100')))

            for price_level, amount_level in relevant_orders:
                price_dec = Decimal(str(price_level))
                amount_dec = Decimal(str(amount_level))

                if side == 'buy' and price_dec > price_limit_buy:
                    self.logger.debug(f"  Liquidity check (BUY): Price level {price_dec:.8f} exceeds limit {price_limit_buy:.8f}. Stopping accumulation.")
                    break
                if side == 'sell' and price_dec < price_limit_sell:
                    self.logger.debug(f"  Liquidity check (SELL): Price level {price_dec:.8f} is below limit {price_limit_sell:.8f}. Stopping accumulation.")
                    break
                
                can_take_from_level = min(amount_dec, amount_base - accumulated_base_amount)
                accumulated_base_amount += can_take_from_level
                accumulated_quote_cost += can_take_from_level * price_dec
                self.logger.debug(f"  Level: P={price_dec:.8f}, A={amount_dec:.8f}. Took: {can_take_from_level:.8f}. Acc.Base: {accumulated_base_amount:.8f}")

                if accumulated_base_amount >= amount_base:
                    break 

            if accumulated_base_amount < amount_base:
                self.logger.warning(f"Insufficient depth for {symbol} on {exchange_id} to fill {amount_base:.8f} {side.upper()} within slippage. "
                                  f"Could only fill {accumulated_base_amount:.8f} at acceptable prices.")
                return False

            avg_fill_price = accumulated_quote_cost / accumulated_base_amount if accumulated_base_amount > 0 else Decimal(0)

            if side == 'buy':
                if avg_fill_price > price_limit_buy:
                    self.logger.warning(f"Liquidity check FAIL (BUY): Avg fill price {avg_fill_price:.8f} for {symbol} on {exchange_id} "
                                      f"exceeds target {price_target:.8f} with slippage limit {price_limit_buy:.8f}.")
                    return False
            elif side == 'sell':
                if avg_fill_price < price_limit_sell:
                    self.logger.warning(f"Liquidity check FAIL (SELL): Avg fill price {avg_fill_price:.8f} for {symbol} on {exchange_id} "
                                      f"is below target {price_target:.8f} with slippage limit {price_limit_sell:.8f}.")
                    return False

            self.logger.info(f"Liquidity check PASS for {symbol} on {exchange_id} ({side}). "
                             f"Able to fill {accumulated_base_amount:.8f} at avg price {avg_fill_price:.8f} (target: {price_target:.8f}).")
            return True

        except Exception as e:
            self.logger.error(f"Error during liquidity check for {symbol} on {exchange_id}: {e}", exc_info=True)
            return False
        

    def __init__(self, exchanges: Dict[str, ccxt_async.Exchange]):
        self.exchanges = exchanges
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stable_candidates: Dict[str, ArbitrageOpportunity] = {}
        self.currencies_cache: Dict[str, Dict[str, Any]] = {}
        self.balance_manager: Optional['BalanceManager'] = None
        self.executor: Optional['Executor'] = None

    def set_balance_manager(self, balance_manager: 'BalanceManager'):
        self.balance_manager = balance_manager

    def set_executor(self, executor: 'Executor'):
        self.executor = executor

    @safe_call_wrapper(default_retval=None)
    async def get_asset_price_in_usdt(self, asset_code: str,
                                      preferred_ref_exchange_id: str = 'binance',
                                      opportunity_context: Optional[ArbitrageOpportunity] = None) -> Optional[Decimal]:
        asset_code_upper = asset_code.upper()
        quote_asset_upper = Config.QUOTE_ASSET.upper()

        if asset_code_upper == quote_asset_upper:
            return Decimal("1.0")

        exchanges_to_try_order: List[Tuple[str, str]] = []

        if opportunity_context:
            if opportunity_context.buy_exchange_id in self.exchanges:
                exchanges_to_try_order.append((opportunity_context.buy_exchange_id, f"opportunity buy_exchange ({opportunity_context.buy_exchange_id})"))
            if opportunity_context.sell_exchange_id in self.exchanges and \
               opportunity_context.sell_exchange_id != opportunity_context.buy_exchange_id:
                exchanges_to_try_order.append((opportunity_context.sell_exchange_id, f"opportunity sell_exchange ({opportunity_context.sell_exchange_id})"))

        if preferred_ref_exchange_id in self.exchanges and \
           preferred_ref_exchange_id not in [ex_id for ex_id, _ in exchanges_to_try_order]:
            exchanges_to_try_order.append((preferred_ref_exchange_id, f"primary reference ({preferred_ref_exchange_id})"))

        tried_ids_set = {ex_id for ex_id, _ in exchanges_to_try_order}
        for ex_id_fallback, ex_inst_fallback in self.exchanges.items():
            if ex_id_fallback not in tried_ids_set and ex_inst_fallback and \
               hasattr(ex_inst_fallback, 'has') and ex_inst_fallback.has.get('fetchTicker') and ex_inst_fallback.markets:
                exchanges_to_try_order.append((ex_id_fallback, f"fallback ({ex_id_fallback})"))

        if not exchanges_to_try_order:
            self.logger.warning(f"No suitable exchanges available to fetch live price for {asset_code_upper}.")
            est_price = Config.ESTIMATED_ASSET_PRICES_USD.get(asset_code_upper)
            if est_price: self.logger.warning(f"Using ESTIMATED_ASSET_PRICES_USD for {asset_code_upper}: ${est_price}"); return est_price
            self.logger.error(f"Failed to get USD price for {asset_code_upper}: No exchanges and no estimate."); return None

        symbol_to_fetch_vs_usdt = f"{asset_code_upper}/{quote_asset_upper}"
        self.logger.debug(f"Attempting to get USD price for {asset_code_upper}. Order of exchanges to try: {[ex_id for ex_id, _ in exchanges_to_try_order]}")

        for ex_id_to_try, source_description in exchanges_to_try_order:
            exchange_instance = self.exchanges.get(ex_id_to_try)
            if not exchange_instance or not exchange_instance.markets:
                continue

            original_timeout = getattr(exchange_instance, 'timeout', 30000)
            exchange_instance.timeout = 60000
            try:
                if symbol_to_fetch_vs_usdt in exchange_instance.markets:
                    self.logger.debug(f"Fetching ticker for {symbol_to_fetch_vs_usdt} on {exchange_instance.id} (Source: {source_description})")
                    ticker = await exchange_instance.fetch_ticker(symbol_to_fetch_vs_usdt)
                    price_value_str = (ticker.get('last') or ticker.get('ask') or ticker.get('bid') or ticker.get('close')) if ticker else None

                    if price_value_str is not None:
                        price_decimal = Decimal(str(price_value_str))
                        if price_decimal > 0:
                            self.logger.info(f"Live price for {symbol_to_fetch_vs_usdt} obtained from {exchange_instance.id} ({source_description}): {price_decimal}")
                            exchange_instance.timeout = original_timeout; return price_decimal
                        else:
                            self.logger.debug(f"Fetched zero or negative price ({price_decimal}) for {symbol_to_fetch_vs_usdt} from {exchange_instance.id}.")
                    else:
                        self.logger.debug(f"No usable price (last/ask/bid/close) in ticker for {symbol_to_fetch_vs_usdt} from {exchange_instance.id}.")
                else:
                    self.logger.debug(f"Market {symbol_to_fetch_vs_usdt} not listed on {exchange_instance.id} ({source_description}).")
            except (ccxt_async.NetworkError, ccxt_async.RequestTimeout, ccxt_async.ExchangeError) as e_ccxt:
                self.logger.warning(f"CCXT error fetching price for {asset_code_upper} on {exchange_instance.id} ({source_description}): {e_ccxt}")
            except (InvalidOperation, TypeError) as e_parse:
                self.logger.warning(f"Error parsing price for {asset_code_upper} on {exchange_instance.id} ({source_description}): {e_parse}")
            except Exception as e_unexp:
                self.logger.warning(f"Unexpected error fetching price for {asset_code_upper} on {exchange_instance.id} ({source_description}): {e_unexp}", exc_info=True)
            finally:
                if exchange_instance:
                    exchange_instance.timeout = original_timeout

        estimated_price = Config.ESTIMATED_ASSET_PRICES_USD.get(asset_code_upper)
        if estimated_price:
            self.logger.warning(f"Failed to fetch live price for {asset_code_upper} from all sources. Using ESTIMATED_ASSET_PRICES_USD: ${estimated_price}.")
            return estimated_price

        self.logger.error(f"Failed to get any USD price (live or estimated) for {asset_code_upper}.")
        return None

    @safe_call_wrapper(default_retval=None)
    async def analyze_and_select_best(self, opportunities: List[ArbitrageOpportunity]) -> Optional[ArbitrageOpportunity]:
        if not opportunities:
            self.logger.debug("Analyzer: No gross opportunities provided to analyze_and_select_best.")
            return None

        valid_opportunities_pre_stability = []
        for opp in opportunities:
            if Config.is_leveraged_token(opp.base_asset):
                self.logger.debug(f"Analyzer: Filtering out leveraged token opportunity: {opp.get_unique_id()}")
                continue
            
            if (opp.buy_exchange_id.lower(), opp.base_asset.upper()) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
               (opp.sell_exchange_id.lower(), opp.base_asset.upper()) in Config.ASSET_UNAVAILABLE_BLACKLIST:
                self.logger.debug(f"Analyzer: Filtering out opportunity {opp.get_unique_id()} due to general ASSET_UNAVAILABLE_BLACKLIST.")
                continue

            valid_opportunities_pre_stability.append(opp)

        if not valid_opportunities_pre_stability:
            self.logger.info("Analyzer: All provided opportunities were leveraged or generally blacklisted. No valid opportunities to analyze.")
            return None

        self._update_stability_counts(valid_opportunities_pre_stability)

        stable_opportunities_gross = [
            opp for opp in self.stable_candidates.values()
            if opp.is_stable
        ]

        if not stable_opportunities_gross:
            self.logger.info("Analyzer: No opportunities met the STABILITY_CYCLES requirement.")
            return None
        
        self.logger.info(f"Analyzer: Found {len(stable_opportunities_gross)} stable gross opportunities. Enriching top {Config.TOP_N_OPPORTUNITIES} by gross profit...")
        stable_opportunities_gross.sort(key=lambda x: x.gross_profit_pct, reverse=True)
        
        enriched_stable_opportunities = await self._enrich_opportunities_with_fees(
            stable_opportunities_gross[:Config.TOP_N_OPPORTUNITIES]
        )

        final_candidate_opportunities: List[ArbitrageOpportunity] = []
        for opp in enriched_stable_opportunities:
            if not opp.potential_networks:
                self.logger.debug(f"Opp {opp.get_unique_id()} has no potential_networks after enrichment. Skipping whitelist/blacklist check.")
                continue

            selected_network_for_this_opp = False
            for net_detail in opp.potential_networks:
                current_path_tuple = (
                    opp.base_asset.upper(),
                    opp.buy_exchange_id.lower(),
                    opp.sell_exchange_id.lower(),
                    net_detail['normalized_name'].upper()
                )

                if current_path_tuple in Config.ARB_PATH_BLACKLIST:
                    self.logger.debug(f"Path {current_path_tuple} for opp {opp.get_unique_id()} is specifically blacklisted. Checking next network option for this opp.")
                    continue

                is_this_network_whitelisted = current_path_tuple in Config.ARB_WHITELIST
                
                if Config.ENFORCE_WHITELIST_ONLY and not is_this_network_whitelisted:
                    self.logger.debug(f"Path {current_path_tuple} for opp {opp.get_unique_id()} is not whitelisted (enforcement on). Checking next network option.")
                    continue
                
                initial_best_net_detail_in_enrich = opp.chosen_network_details
                
                if net_detail['normalized_name'] != (initial_best_net_detail_in_enrich.get('normalized_name') if initial_best_net_detail_in_enrich else None):
                    self.logger.debug(f"Re-evaluating net profit for {opp.get_unique_id()} with network {net_detail['normalized_name']} (was previously {initial_best_net_detail_in_enrich.get('normalized_name') if initial_best_net_detail_in_enrich else 'None'}) due to whitelist/blacklist selection.")
                    new_withdrawal_fee_usd = None
                    native_fee = net_detail['fee_native']
                    fee_currency = net_detail.get('fee_currency', opp.base_asset)

                    if fee_currency.upper() == opp.base_asset.upper():
                        new_withdrawal_fee_usd = native_fee * opp.buy_price
                    elif fee_currency.upper() == Config.QUOTE_ASSET.upper():
                        new_withdrawal_fee_usd = native_fee
                    else:
                        fee_curr_price_usdt = await self.get_asset_price_in_usdt(fee_currency, preferred_ref_exchange_id=opp.buy_exchange_id, opportunity_context=opp)
                        if fee_curr_price_usdt and fee_curr_price_usdt > 0:
                            new_withdrawal_fee_usd = native_fee * fee_curr_price_usdt
                        else:
                            self.logger.warning(f"Could not get price for fee currency {fee_currency} for {opp.get_unique_id()}. Using default high fee.")
                            new_withdrawal_fee_usd = Config.ESTIMATED_WITHDRAWAL_FEES_USD.get('DEFAULT_ASSET_FEE')
                    
                    opp.withdrawal_fee_usd = new_withdrawal_fee_usd
                    if new_withdrawal_fee_usd is not None and Config.TRADE_AMOUNT_USD > 0:
                        withdrawal_fee_pct_of_trade = (new_withdrawal_fee_usd / Config.TRADE_AMOUNT_USD) * Decimal('100')
                        opp.net_profit_pct = opp.gross_profit_pct - (opp.buy_fee_pct or 0) - (opp.sell_fee_pct or 0) - withdrawal_fee_pct_of_trade
                    else:
                        opp.net_profit_pct = None
                
                opp.chosen_network = net_detail['normalized_name']
                opp.chosen_network_details = net_detail

                final_candidate_opportunities.append(opp)
                selected_network_for_this_opp = True
                break

            if not selected_network_for_this_opp:
                 self.logger.debug(f"Opp {opp.get_unique_id()} had no valid (whitelisted/not_blacklisted) networks in its potential_networks list after filtering. Skipping this opp.")
        
        if not final_candidate_opportunities:
            self.logger.info("Analyzer: No opportunities remained after whitelist/blacklist filtering.")
            return None

        net_profitable_opportunities = [
            opp for opp in final_candidate_opportunities
            if opp.net_profit_pct is not None and opp.net_profit_pct >= Config.MIN_PROFIT_THRESHOLD_NET
        ]

        if not net_profitable_opportunities:
            self.logger.info("Analyzer: No opportunities remained net profitable after whitelist/blacklist and fee calculation.")
            return None

        net_profitable_opportunities.sort(key=lambda x: x.net_profit_pct, reverse=True)

        best_opportunity_final: Optional[ArbitrageOpportunity] = None
        for potential_best_opp in net_profitable_opportunities:
            if not potential_best_opp.chosen_network_details:
                 self.logger.warning(f"Opportunity {potential_best_opp.get_unique_id()} has no chosen_network_details. Skipping liquidity check.")
                 continue

            if self.executor and hasattr(self.executor, '_check_liquidity_for_trade_leg'):
                self.logger.debug(f"Performing final liquidity check for selected opportunity: {potential_best_opp.get_unique_id()}")
                
                buy_liquid = await self.executor._check_liquidity_for_trade_leg(
                    potential_best_opp.buy_exchange_id, potential_best_opp.symbol, 'buy',
                    Config.TRADE_AMOUNT_USD, potential_best_opp.buy_price, is_base_amount=False
                )
                
                approx_base_amount = Config.TRADE_AMOUNT_USD / potential_best_opp.buy_price if potential_best_opp.buy_price > 0 else Decimal(0)
                sell_liquid = False
                if approx_base_amount > 0:
                    sell_liquid = await self.executor._check_liquidity_for_trade_leg(
                        potential_best_opp.sell_exchange_id, potential_best_opp.symbol, 'sell',
                        approx_base_amount, potential_best_opp.sell_price, is_base_amount=True
                    )
                
                potential_best_opp.is_liquid_enough_for_trade = buy_liquid and sell_liquid
                if potential_best_opp.is_liquid_enough_for_trade:
                    best_opportunity_final = potential_best_opp
                    break
                else:
                    self.logger.warning(f"Opportunity {potential_best_opp.get_unique_id()} failed final liquidity check. Buy liquid: {buy_liquid}, Sell liquid: {sell_liquid}. Trying next.")
            else:
                self.logger.warning("Executor or _check_liquidity_for_trade_leg not available. Selecting best opportunity without final liquidity check.")
                potential_best_opp.is_liquid_enough_for_trade = True
                best_opportunity_final = potential_best_opp
                break
        
        if not best_opportunity_final:
            self.logger.info("Analyzer: No net profitable opportunities passed the final liquidity check (if performed).")
            return None

        self.logger.info(f"🏆 Best Full Arbitrage Opportunity Selected: {best_opportunity_final.get_unique_id()} | Symbol: {best_opportunity_final.symbol} | "
                         f"Gross Profit: {best_opportunity_final.gross_profit_pct:.3f}% -> Net Profit: {best_opportunity_final.net_profit_pct:.3f}% "
                         f"(Buy: {best_opportunity_final.buy_exchange_id} @ {best_opportunity_final.buy_price:.8f}, Sell: {best_opportunity_final.sell_exchange_id} @ {best_opportunity_final.sell_price:.8f}) "
                         f"Withdrawal Fee (est.): ${best_opportunity_final.withdrawal_fee_usd if best_opportunity_final.withdrawal_fee_usd is not None else 'N/A':.2f} via {best_opportunity_final.chosen_network or 'N/A'} "
                         f"Liquid: {best_opportunity_final.is_liquid_enough_for_trade}")

        if best_opportunity_final.get_unique_id() in self.stable_candidates:
            del self.stable_candidates[best_opportunity_final.get_unique_id()]
            self.logger.debug(f"Removed chosen opportunity {best_opportunity_final.get_unique_id()} from stable_candidates.")

        return best_opportunity_final

    def _update_stability_counts(self, current_scan_opportunities: List[ArbitrageOpportunity]):
        current_opportunity_ids_set = {opp.get_unique_id() for opp in current_scan_opportunities}

        for opp in current_scan_opportunities:
            uid = opp.get_unique_id()
            if uid in self.stable_candidates:
                self.stable_candidates[uid].stability_count += 1
                self.stable_candidates[uid].buy_price = opp.buy_price
                self.stable_candidates[uid].sell_price = opp.sell_price
                self.stable_candidates[uid].gross_profit_pct = opp.gross_profit_pct
            else:
                self.stable_candidates[uid] = opp
                self.stable_candidates[uid].stability_count = 1

            if self.stable_candidates[uid].stability_count >= Config.STABILITY_CYCLES:
                self.stable_candidates[uid].is_stable = True
        
        stale_candidate_uids = [uid for uid in self.stable_candidates if uid not in current_opportunity_ids_set]
        for uid_to_remove in stale_candidate_uids:
            removed_candidate = self.stable_candidates.pop(uid_to_remove, None)
            if removed_candidate:
                 self.logger.debug(f"Removed stale/disappeared candidate: {removed_candidate.get_unique_id()} (was stable: {removed_candidate.is_stable}, count: {removed_candidate.stability_count})")
        if stale_candidate_uids:
            self.logger.debug(f"Removed {len(stale_candidate_uids)} stale/disappeared candidates from stability tracking.")

    @safe_call_wrapper(default_retval=[])
    async def _enrich_opportunities_with_fees(self, opportunities: List[ArbitrageOpportunity]) -> List[ArbitrageOpportunity]:
        self.logger.debug(f"Enriching {len(opportunities)} opportunities with detailed fee data (including base asset withdrawal)...")
        enriched_list: List[ArbitrageOpportunity] = []

        await self._cache_all_currencies_info_for_enrichment(opportunities)

        for opp in opportunities:
            self.logger.debug(f"Enriching details for opportunity: {opp.get_unique_id()}")
            try:
                buy_ex_inst = self.exchanges.get(opp.buy_exchange_id)
                sell_ex_inst = self.exchanges.get(opp.sell_exchange_id)

                if not buy_ex_inst or not sell_ex_inst or \
                   not buy_ex_inst.markets or opp.symbol not in buy_ex_inst.markets or \
                   not sell_ex_inst.markets or opp.symbol not in sell_ex_inst.markets:
                    self.logger.warning(f"Market data or exchange instance missing for {opp.get_unique_id()}. Cannot enrich. Skipping.")
                    opp.net_profit_pct = None
                    enriched_list.append(opp)
                    continue
                
                buy_market_details = buy_ex_inst.markets[opp.symbol]
                sell_market_details = sell_ex_inst.markets[opp.symbol]

                default_taker_fee_rate = Decimal('0.001')

                buy_taker_fee_rate_str = buy_market_details.get('taker', default_taker_fee_rate)
                sell_taker_fee_rate_str = sell_market_details.get('taker', default_taker_fee_rate)

                try:
                    opp.buy_fee_pct = Decimal(str(buy_taker_fee_rate_str)) * Decimal('100')
                    opp.sell_fee_pct = Decimal(str(sell_taker_fee_rate_str)) * Decimal('100')
                except (InvalidOperation, TypeError) as e_fee_parse:
                    self.logger.warning(f"Could not parse taker fee for {opp.get_unique_id()}: {e_fee_parse}. Using default 0.1%. Buy raw: '{buy_taker_fee_rate_str}', Sell raw: '{sell_taker_fee_rate_str}'")
                    opp.buy_fee_pct = default_taker_fee_rate * Decimal('100')
                    opp.sell_fee_pct = default_taker_fee_rate * Decimal('100')

                approx_base_to_withdraw = Config.TRADE_AMOUNT_USD / opp.buy_price if opp.buy_price > 0 else Decimal(0)
                
                potential_networks_list = await self._select_optimal_network_for_transfer(
                    asset_code=opp.base_asset,
                    from_exchange_id=opp.buy_exchange_id,
                    to_exchange_id=opp.sell_exchange_id,
                    amount_native_to_withdraw_optional=approx_base_to_withdraw
                )
                opp.potential_networks = potential_networks_list if potential_networks_list else []

                opp.withdrawal_fee_usd = None
                opp.chosen_network = None
                opp.chosen_network_details = None

                if opp.potential_networks:
                    best_network_for_fee_calc = opp.potential_networks[0]
                    opp.chosen_network = best_network_for_fee_calc['normalized_name']
                    opp.chosen_network_details = best_network_for_fee_calc

                    native_fee = best_network_for_fee_calc['fee_native']
                    fee_currency = best_network_for_fee_calc.get('fee_currency', opp.base_asset)

                    if fee_currency.upper() == opp.base_asset.upper():
                        opp.withdrawal_fee_usd = native_fee * opp.buy_price
                    elif fee_currency.upper() == Config.QUOTE_ASSET.upper():
                        opp.withdrawal_fee_usd = native_fee
                    else:
                        fee_curr_price_usdt = await self.get_asset_price_in_usdt(fee_currency, preferred_ref_exchange_id=opp.buy_exchange_id, opportunity_context=opp)
                        if fee_curr_price_usdt and fee_curr_price_usdt > 0:
                            opp.withdrawal_fee_usd = native_fee * fee_curr_price_usdt
                        else:
                            self.logger.warning(f"Could not get price for fee currency {fee_currency} to calculate withdrawal fee in USD for {opp.get_unique_id()}. Estimating high.")
                            opp.withdrawal_fee_usd = Config.ESTIMATED_WITHDRAWAL_FEES_USD.get('DEFAULT_ASSET_FEE')
                else:
                    self.logger.warning(f"No suitable networks found for withdrawing {opp.base_asset} from {opp.buy_exchange_id} to {opp.sell_exchange_id}. Using default high withdrawal fee estimate.")
                    asset_default_fees = Config.ESTIMATED_WITHDRAWAL_FEES_USD.get(opp.base_asset.upper(), {})
                    opp.withdrawal_fee_usd = asset_default_fees.get('DEFAULT', Config.ESTIMATED_WITHDRAWAL_FEES_USD['DEFAULT_ASSET_FEE'])

                if opp.withdrawal_fee_usd is not None and Config.TRADE_AMOUNT_USD > 0:
                    withdrawal_fee_pct_of_trade = (opp.withdrawal_fee_usd / Config.TRADE_AMOUNT_USD) * Decimal('100')
                    
                    opp.net_profit_pct = opp.gross_profit_pct - \
                                         (opp.buy_fee_pct or 0) - \
                                         (opp.sell_fee_pct or 0) - \
                                         withdrawal_fee_pct_of_trade
                    
                    self.logger.info(f"Enriched (Full Arbitrage): {opp.get_unique_id()} | "
                                     f"Gross: {opp.gross_profit_pct:.3f}%, BuyFee: {opp.buy_fee_pct or 0:.3f}%, SellFee: {opp.sell_fee_pct or 0:.3f}%, "
                                     f"WithdrawFee ({opp.base_asset} via {opp.chosen_network or 'N/A'}): ~${opp.withdrawal_fee_usd:.2f} ({withdrawal_fee_pct_of_trade:.3f}%) "
                                     f"-> Net Profit Pct: {opp.net_profit_pct:.3f}%")
                else:
                    opp.net_profit_pct = None
                    self.logger.warning(f"Net profit not calculated for {opp.get_unique_id()} due to missing withdrawal fee calculation or zero trade amount.")
                
                enriched_list.append(opp)

            except Exception as e_enrich:
                self.logger.error(f"Error enriching opportunity {opp.get_unique_id()}: {e_enrich}", exc_info=True)
                opp.net_profit_pct = None
                enriched_list.append(opp)
        return enriched_list

    @safe_call_wrapper(default_retval=None)
    async def _cache_all_currencies_info_for_enrichment(self, opportunities: List[ArbitrageOpportunity]):
        exchanges_needing_currency_fetch = set()
        for ex_id in self.exchanges.keys():
             if ex_id not in self.currencies_cache or not self.currencies_cache.get(ex_id):
                exchanges_needing_currency_fetch.add(ex_id)

        for opp in opportunities:
            if opp.buy_exchange_id not in self.currencies_cache and opp.buy_exchange_id in self.exchanges:
                exchanges_needing_currency_fetch.add(opp.buy_exchange_id)
            if opp.sell_exchange_id not in self.currencies_cache and opp.sell_exchange_id in self.exchanges:
                 exchanges_needing_currency_fetch.add(opp.sell_exchange_id)


        if not exchanges_needing_currency_fetch:
            self.logger.debug("All required currency information for enrichment/network selection appears to be cached.")
            return

        self.logger.info(f"Fetching/refreshing currency information from CCXT for exchanges: {list(exchanges_needing_currency_fetch)}")
        currency_fetch_tasks: Dict[str, asyncio.Task] = {}
        original_timeouts_curr = {}

        for ex_id in exchanges_needing_currency_fetch:
            ex_inst = self.exchanges.get(ex_id)
            if ex_inst and hasattr(ex_inst, 'has') and ex_inst.has.get('fetchCurrencies'):
                original_timeouts_curr[ex_id] = getattr(ex_inst, 'timeout', 30000)
                ex_inst.timeout = 90000
                self.logger.debug(f"Fetching currencies for {ex_id} with timeout {ex_inst.timeout}ms")
                currency_fetch_tasks[ex_id] = asyncio.create_task(ex_inst.fetch_currencies())
            else:
                self.logger.warning(f"Exchange {ex_id} does not support fetchCurrencies or instance not available. Currency cache for it will be empty or unchanged.")
                self.currencies_cache[ex_id] = self.currencies_cache.get(ex_id, {})

        if currency_fetch_tasks:
            task_keys_ordered = list(currency_fetch_tasks.keys())
            results = await asyncio.gather(*currency_fetch_tasks.values(), return_exceptions=True)
            for ex_id, result in zip(task_keys_ordered, results):
                ex_inst_after_fetch = self.exchanges.get(ex_id)
                if ex_inst_after_fetch and ex_id in original_timeouts_curr:
                    setattr(ex_inst_after_fetch, 'timeout', original_timeouts_curr[ex_id])

                if not isinstance(result, Exception) and result and isinstance(result, dict):
                    self.currencies_cache[ex_id] = result
                    self.logger.info(f"Successfully cached/updated currency information for {ex_id} (Total currencies: {len(result)}).")
                elif isinstance(result, Exception):
                    self.logger.error(f"Failed to fetch currencies for {ex_id}: {result}. Existing cache (if any) retained.")
                    self.currencies_cache[ex_id] = self.currencies_cache.get(ex_id, {})
                else:
                    self.logger.warning(f"No currency data returned or unexpected format from {ex_id}. Result type: {type(result)}. Using existing/empty cache.")
                    self.currencies_cache[ex_id] = self.currencies_cache.get(ex_id, {})
        self.logger.debug("Finished caching/refreshing currency information from CCXT.")

    @safe_call_wrapper(default_retval=[])
    async def _select_optimal_network_for_transfer(self,
                                               asset_code: str,
                                               from_exchange_id: str,
                                               to_exchange_id: str,
                                               amount_native_to_withdraw_optional: Optional[Decimal] = None
                                               ) -> List[Dict[str, Any]]:
        self.logger.debug(f"Selecting optimal transfer network(s) for {asset_code} from {from_exchange_id} to {to_exchange_id}"
                          f"{f' (amount: {amount_native_to_withdraw_optional})' if amount_native_to_withdraw_optional else ''}")
        asset_code_upper = asset_code.upper()

        # --- ИСПРАВЛЕНО: Применение нового правила TOKEN_NETWORK_RESTRICTIONS ---
        restriction_key = (from_exchange_id.lower(), asset_code_upper)
        allowed_networks_for_path = Config.TOKEN_NETWORK_RESTRICTIONS.get(restriction_key)
        if allowed_networks_for_path:
            self.logger.info(f"Applying network restriction for {asset_code_upper} from {from_exchange_id}: Only networks {allowed_networks_for_path} are allowed.")

        if (from_exchange_id.lower(), asset_code_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
           (to_exchange_id.lower(), asset_code_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST:
            self.logger.warning(f"Asset {asset_code_upper} transfer involving {from_exchange_id} or {to_exchange_id} is generally blacklisted. No networks will be selected.")
            return []

        potential_from_networks_details: List[Dict[str, Any]] = []
        
        from_ex_id_lower = from_exchange_id.lower()
        from_asset_direct_data = Config.LOADED_EXCHANGE_FEES_DATA.get(from_ex_id_lower, {}).get(asset_code_upper, {})
        from_networks_direct = from_asset_direct_data.get('networks', {}) if isinstance(from_asset_direct_data, dict) else {}

        for norm_net_name_direct, direct_net_info in from_networks_direct.items():
            if not isinstance(direct_net_info, dict) or direct_net_info.get('fee') is None:
                continue
            if not direct_net_info.get('can_withdraw', True):
                self.logger.debug(f"JSON Network (FROM): '{direct_net_info.get('original_name_from_file', norm_net_name_direct)}' on {from_exchange_id} for {asset_code_upper} is marked 'can_withdraw: false'. Skipping.")
                continue

            # --- ИСПРАВЛЕНО: Проверка на соответствие правилу TOKEN_NETWORK_RESTRICTIONS ---
            if allowed_networks_for_path and norm_net_name_direct not in allowed_networks_for_path:
                self.logger.debug(f"Skipping network '{norm_net_name_direct}' from direct config for {asset_code_upper} from {from_exchange_id} due to restriction. Allowed: {allowed_networks_for_path}")
                continue

            original_name_from_direct_data = direct_net_info.get('original_name_from_file', norm_net_name_direct)
            min_wd_val = direct_net_info.get('min_withdraw', Decimal('0'))
            try:
                fee_dec = Decimal(str(direct_net_info['fee']))
                min_wd_dec = Decimal(str(min_wd_val)) if min_wd_val is not None else Decimal('0')
            except (InvalidOperation, TypeError) as e_parse:
                self.logger.warning(f"Error parsing fee/min_withdrawal for direct_data network '{original_name_from_direct_data}' (Asset: {asset_code_upper}, Ex: {from_exchange_id}): {e_parse}. Skipping.")
                continue

            potential_from_networks_details.append({
                'withdraw_network_code_on_from_ex': original_name_from_direct_data,
                'normalized_name': norm_net_name_direct,
                'fee_native': fee_dec,
                'fee_currency': asset_code_upper,
                'min_withdrawal_native': min_wd_dec,
                'source_of_fee': "DIRECT_CONFIG_DATA",
                'can_withdraw': True
            })
            self.logger.debug(f"Added network from DIRECT_CONFIG_DATA (FROM): '{original_name_from_direct_data}' (Normalized: {norm_net_name_direct}) for {asset_code_upper} on {from_exchange_id}.")

        if from_exchange_id not in self.currencies_cache or not self.currencies_cache.get(from_exchange_id):
            await self._cache_all_currencies_info_for_enrichment([])

        from_ex_ccxt_currencies = self.currencies_cache.get(from_exchange_id, {})
        from_asset_ccxt_details = from_ex_ccxt_currencies.get(asset_code_upper, {}) if isinstance(from_ex_ccxt_currencies, dict) else {}
        from_networks_ccxt = from_asset_ccxt_details.get('networks', {}) if isinstance(from_asset_ccxt_details, dict) else {}
        
        for ccxt_net_original_name, ccxt_net_info in from_networks_ccxt.items():
            if not isinstance(ccxt_net_info, dict): continue
            
            normalized_ccxt_net_name = Config.normalize_network_name_for_config(ccxt_net_original_name)
            
            # --- ИСПРАВЛЕНО: Проверка на соответствие правилу TOKEN_NETWORK_RESTRICTIONS ---
            if allowed_networks_for_path and normalized_ccxt_net_name not in allowed_networks_for_path:
                self.logger.debug(f"Skipping network '{normalized_ccxt_net_name}' from CCXT for {asset_code_upper} from {from_exchange_id} due to restriction. Allowed: {allowed_networks_for_path}")
                continue

            if any(pfn['normalized_name'] == normalized_ccxt_net_name for pfn in potential_from_networks_details):
                self.logger.debug(f"Network {normalized_ccxt_net_name} (from CCXT original: {ccxt_net_original_name}) on {from_exchange_id} already covered by direct config data. Skipping CCXT version for FROM_EX.")
                continue

            is_ccxt_net_active = ccxt_net_info.get('active', True)
            is_ccxt_net_withdrawable = ccxt_net_info.get('withdraw', True)
            if not (is_ccxt_net_active and is_ccxt_net_withdrawable):
                self.logger.debug(f"CCXT Network (FROM): '{ccxt_net_original_name}' on {from_exchange_id} for {asset_code_upper} is inactive or not withdrawable. Skipping.")
                continue

            fee_native_val = ccxt_net_info.get('fee')
            fee_currency_code = ccxt_net_info.get('feeCurrency', asset_code_upper)
            min_withdrawal_native_val = ccxt_net_info.get('limits', {}).get('withdraw', {}).get('min')
            
            if fee_native_val is None:
                self.logger.debug(f"CCXT Network (FROM): '{ccxt_net_original_name}' on {from_exchange_id} for {asset_code_upper} has no fee information. Skipping.")
                continue
            try:
                fee_decimal = Decimal(str(fee_native_val))
                min_withdrawal_decimal = Decimal(str(min_withdrawal_native_val)) if min_withdrawal_native_val is not None else Decimal(0)
            except (InvalidOperation, TypeError) as e_parse:
                self.logger.warning(f"Error parsing fee/min_withdrawal for CCXT network '{ccxt_net_original_name}' (Asset: {asset_code_upper}, Ex: {from_exchange_id}): {e_parse}. Skipping.")
                continue
            
            potential_from_networks_details.append({
                'withdraw_network_code_on_from_ex': ccxt_net_original_name,
                'normalized_name': normalized_ccxt_net_name,
                'fee_native': fee_decimal,
                'fee_currency': fee_currency_code.upper(),
                'min_withdrawal_native': min_withdrawal_decimal,
                'source_of_fee': "CCXT_API_DATA",
                'can_withdraw': True
            })
            self.logger.debug(f"Added network from CCXT_API_DATA (FROM): '{ccxt_net_original_name}' (Normalized: {normalized_ccxt_net_name}) for {asset_code_upper} on {from_exchange_id}.")
        
        self.logger.debug(f"[{from_exchange_id}->{to_exchange_id}|{asset_code_upper}] Found {len(potential_from_networks_details)} potential WITHDRAWAL networks on {from_exchange_id}: "
                          f"{[d['normalized_name'] for d in potential_from_networks_details]}")

        if not potential_from_networks_details:
            self.logger.warning(f"No networks found for withdrawing {asset_code_upper} from {from_exchange_id} (or all are disabled/blacklisted). Cannot select optimal network."); return []

        active_deposit_normalized_nets_on_to_ex: Dict[str, str] = {}
        
        to_ex_id_lower = to_exchange_id.lower()
        to_asset_direct_data = Config.LOADED_EXCHANGE_FEES_DATA.get(to_ex_id_lower, {}).get(asset_code_upper, {})
        to_networks_direct_deposit = to_asset_direct_data.get('networks', {}) if isinstance(to_asset_direct_data, dict) else {}

        for norm_net_name_direct_to, direct_to_info in to_networks_direct_deposit.items():
            if isinstance(direct_to_info, dict) and direct_to_info.get('can_deposit', True):
                if norm_net_name_direct_to not in active_deposit_normalized_nets_on_to_ex:
                    active_deposit_normalized_nets_on_to_ex[norm_net_name_direct_to] = direct_to_info.get('original_name_from_file', norm_net_name_direct_to)
                    self.logger.debug(f"Found deposit-enabled network from DIRECT_CONFIG_DATA (TO): '{direct_to_info.get('original_name_from_file', norm_net_name_direct_to)}' (Normalized: {norm_net_name_direct_to}) for {asset_code_upper} on {to_exchange_id}.")
            elif isinstance(direct_to_info, dict) and not direct_to_info.get('can_deposit', True):
                 self.logger.debug(f"JSON Network (TO): '{direct_to_info.get('original_name_from_file', norm_net_name_direct_to)}' on {to_exchange_id} for {asset_code_upper} is marked 'can_deposit: false'. Skipping.")

        if to_exchange_id not in self.currencies_cache or not self.currencies_cache.get(to_exchange_id):
            await self._cache_all_currencies_info_for_enrichment([])

        to_ex_ccxt_currencies = self.currencies_cache.get(to_exchange_id, {})
        to_asset_ccxt_details = to_ex_ccxt_currencies.get(asset_code_upper, {}) if isinstance(to_ex_ccxt_currencies, dict) else {}
        to_networks_ccxt = to_asset_ccxt_details.get('networks', {}) if isinstance(to_asset_ccxt_details, dict) else {}

        for to_net_orig_ccxt, to_net_info_ccxt in to_networks_ccxt.items():
            if isinstance(to_net_info_ccxt, dict) and \
               to_net_info_ccxt.get('active', True) and \
               to_net_info_ccxt.get('deposit', True):
                norm_name_to = Config.normalize_network_name_for_config(to_net_orig_ccxt)
                if norm_name_to not in active_deposit_normalized_nets_on_to_ex:
                    active_deposit_normalized_nets_on_to_ex[norm_name_to] = to_net_orig_ccxt
                    self.logger.debug(f"Found deposit-enabled network from CCXT_API_DATA (TO): '{to_net_orig_ccxt}' (Normalized: {norm_name_to}) for {asset_code_upper} on {to_exchange_id}.")
            elif isinstance(to_net_info_ccxt, dict) and not (to_net_info_ccxt.get('active', True) and to_net_info_ccxt.get('deposit', True)):
                 self.logger.debug(f"CCXT Network (TO): '{to_net_orig_ccxt}' on {to_exchange_id} for {asset_code_upper} is inactive or not depositable. Skipping.")


        self.logger.debug(f"[{from_exchange_id}->{to_exchange_id}|{asset_code_upper}] Found {len(active_deposit_normalized_nets_on_to_ex)} active DEPOSIT networks on {to_exchange_id}: "
                          f"{list(active_deposit_normalized_nets_on_to_ex.keys())}")

        if not active_deposit_normalized_nets_on_to_ex:
            self.logger.warning(f"No active networks found for depositing {asset_code_upper} to {to_exchange_id}. Cannot find transfer path.")
            return []
        
        final_candidate_networks: List[Dict[str, Any]] = []
        for from_net_detail in potential_from_networks_details:
            normalized_name_to_match = from_net_detail['normalized_name']

            if normalized_name_to_match in active_deposit_normalized_nets_on_to_ex and \
               normalized_name_to_match != 'UNKNOWN_NETWORK':

                if amount_native_to_withdraw_optional is not None and \
                   from_net_detail['min_withdrawal_native'] > 0 and \
                   amount_native_to_withdraw_optional < from_net_detail['min_withdrawal_native']:
                    self.logger.debug(f"  Skipping network {normalized_name_to_match} for {asset_code_upper} transfer: "
                                      f"Amount {amount_native_to_withdraw_optional} < Min Withdrawal {from_net_detail['min_withdrawal_native']}")
                    continue

                deposit_net_code_on_to_ex = active_deposit_normalized_nets_on_to_ex.get(normalized_name_to_match)
                if not deposit_net_code_on_to_ex:
                    self.logger.warning(f"Internal inconsistency: Could not find API deposit code for normalized network '{normalized_name_to_match}' on {to_exchange_id}. Skipping this path for safety.")
                    continue
                
                final_candidate_networks.append({
                    'withdraw_network_code_on_from_ex': from_net_detail['withdraw_network_code_on_from_ex'],
                    'deposit_network_code_on_to_ex': deposit_net_code_on_to_ex,
                    'normalized_name': normalized_name_to_match,
                    'fee_native': from_net_detail['fee_native'],
                    'fee_currency': from_net_detail['fee_currency'],
                    'min_withdrawal_native': from_net_detail['min_withdrawal_native'],
                    'source_of_fee': from_net_detail['source_of_fee'],
                    'priority_score_token': Config.get_network_priority_score(normalized_name_to_match, asset_code_upper),
                    'priority_score_general': Config.get_network_priority_score(normalized_name_to_match)
                })
                self.logger.debug(f"  Found compatible and ENABLED transfer path for {asset_code_upper}: "
                                  f"From Ex: {from_exchange_id} (Withdraw Net Code: {from_net_detail['withdraw_network_code_on_from_ex']}) -> "
                                  f"To Ex: {to_exchange_id} (Deposit Net Code for fetchDepositAddress: {deposit_net_code_on_to_ex}). "
                                  f"Normalized: {normalized_name_to_match}, Fee: {from_net_detail['fee_native']} {from_net_detail['fee_currency']}")

        if not final_candidate_networks:
            self.logger.warning(f"No common, active, and enabled (withdraw & deposit) networks found for transferring {asset_code_upper} from {from_exchange_id} to {to_exchange_id} after detailed check, or amount too low for all.")
            return []

        unique_fee_currencies = set(net.get('fee_currency', asset_code_upper) for net in final_candidate_networks)
        fee_currency_prices_in_usd: Dict[str, Optional[Decimal]] = {}
        for fee_curr_code in unique_fee_currencies:
            price_in_usdt = await self.get_asset_price_in_usdt(fee_curr_code, preferred_ref_exchange_id=from_exchange_id)
            if price_in_usdt and price_in_usdt > 0:
                 fee_currency_prices_in_usd[fee_curr_code.upper()] = price_in_usdt
            else:
                estimated_fee_curr_price = Config.ESTIMATED_ASSET_PRICES_USD.get(fee_curr_code.upper())
                if estimated_fee_curr_price:
                    fee_currency_prices_in_usd[fee_curr_code.upper()] = estimated_fee_curr_price
                    self.logger.debug(f"Used ESTIMATED price for fee currency {fee_curr_code}: ${estimated_fee_curr_price}")
                else:
                     self.logger.warning(f"Could not determine USD price for fee currency '{fee_curr_code}'. Withdrawal fee sorting for this currency might be inaccurate.")
                     fee_currency_prices_in_usd[fee_curr_code.upper()] = None

        def sort_key_for_networks(network_option: Dict[str, Any]) -> Tuple[Decimal, int, int]:
            fee_native = network_option['fee_native']
            fee_curr_upper = network_option.get('fee_currency', asset_code_upper).upper()
            fee_in_usd_equivalent = Decimal('Infinity')

            price_of_fee_asset_in_usd = fee_currency_prices_in_usd.get(fee_curr_upper)

            if price_of_fee_asset_in_usd and price_of_fee_asset_in_usd > 0:
                fee_in_usd_equivalent = fee_native * price_of_fee_asset_in_usd
            elif fee_curr_upper == Config.QUOTE_ASSET.upper():
                 fee_in_usd_equivalent = fee_native
            else:
                self.logger.debug(f"Network '{network_option['normalized_name']}' (fee currency '{fee_curr_upper}') - USD price for fee currency unknown. Sorting with high effective fee.")

            token_pref_score = network_option['priority_score_token']
            general_pref_score = network_option['priority_score_general']
            
            return (fee_in_usd_equivalent, token_pref_score, general_pref_score)

        final_candidate_networks.sort(key=sort_key_for_networks)

        if not final_candidate_networks:
            self.logger.warning(f"No candidate networks remained after sorting for {asset_code_upper} from {from_exchange_id} to {to_exchange_id}.")
            return []

        num_to_log = min(3, len(final_candidate_networks))
        self.logger.info(f"Top {num_to_log} potential networks for {asset_code_upper} ({from_exchange_id} -> {to_exchange_id}) after sorting by fee/preference:")
        for i, net_opt in enumerate(final_candidate_networks[:num_to_log]):
            fee_usd_log_detail = ""
            fee_price = fee_currency_prices_in_usd.get(net_opt['fee_currency'].upper())
            if fee_price and fee_price > 0:
                fee_usd_log_detail = f" (~${(net_opt['fee_native'] * fee_price):.2f} USD)"
            self.logger.info(f"  {i+1}. Normalized: '{net_opt['normalized_name']}', "
                             f"Withdraw Code (on {from_exchange_id}): '{net_opt['withdraw_network_code_on_from_ex']}', "
                             f"Deposit Code (on {to_exchange_id}): '{net_opt['deposit_network_code_on_to_ex']}', "
                             f"Fee: {net_opt['fee_native']} {net_opt.get('fee_currency', asset_code_upper)}{fee_usd_log_detail}, "
                             f"MinW: {net_opt['min_withdrawal_native']} {asset_code_upper}, "
                             f"Scores (Tok/Gen): {net_opt['priority_score_token']}/{net_opt['priority_score_general']}")

        return final_candidate_networks

    async def _check_orderbook_liquidity(self, exchange_id: str, symbol: str, side: str,
                                         amount_base: Decimal, price_target: Decimal,
                                         depth_pct_allowed_slippage: Decimal = Decimal("1.0")
                                         ) -> bool:
        self.logger.debug(f"Liquidity check for {exchange_id} - {symbol} ({side}) "
                          f"Amount: {amount_base:.8f} Base, Price Target: {price_target:.8f}. "
                          f"Slippage allowance: {depth_pct_allowed_slippage}%, Min Market Liquidity: ${Config.MIN_LIQUIDITY_USD}")

        exchange = self.exchanges.get(exchange_id)
        if not exchange or not exchange.has.get('fetchOrderBook'):
            self.logger.warning(f"Exchange {exchange_id} does not support fetchOrderBook or not found. Assuming liquid for {symbol}.")
            return True

        try:
            orderbook = await exchange.fetch_order_book(symbol, limit=20)
            relevant_orders = orderbook['asks' if side == 'buy' else 'bids']

            if not relevant_orders:
                self.logger.warning(f"No {'asks' if side == 'buy' else 'bids'} found in orderbook for {symbol} on {exchange_id}. Assuming not liquid enough.")
                return False

            total_visible_liquidity_usd = sum(Decimal(str(p)) * Decimal(str(a)) for p, a in relevant_orders)
            if total_visible_liquidity_usd < Config.MIN_LIQUIDITY_USD:
                self.logger.warning(
                    f"Liquidity check FAIL (Market Depth): Total visible liquidity for {symbol} on {exchange_id} "
                    f"is only ~${total_visible_liquidity_usd:.2f}, which is below the required minimum of ${Config.MIN_LIQUIDITY_USD}."
                )
                return False
            
            accumulated_base_amount = Decimal('0')
            accumulated_quote_cost = Decimal('0')
            price_limit_buy = price_target * (Decimal('1') + (depth_pct_allowed_slippage / Decimal('100')))
            price_limit_sell = price_target * (Decimal('1') - (depth_pct_allowed_slippage / Decimal('100')))

            for price_level, amount_level in relevant_orders:
                price_dec = Decimal(str(price_level))
                amount_dec = Decimal(str(amount_level))

                if side == 'buy' and price_dec > price_limit_buy:
                    self.logger.debug(f"  Liquidity check (BUY): Price level {price_dec:.8f} exceeds limit {price_limit_buy:.8f}. Stopping accumulation.")
                    break
                if side == 'sell' and price_dec < price_limit_sell:
                    self.logger.debug(f"  Liquidity check (SELL): Price level {price_dec:.8f} is below limit {price_limit_sell:.8f}. Stopping accumulation.")
                    break
                
                can_take_from_level = min(amount_dec, amount_base - accumulated_base_amount)
                accumulated_base_amount += can_take_from_level
                accumulated_quote_cost += can_take_from_level * price_dec
                self.logger.debug(f"  Level: P={price_dec:.8f}, A={amount_dec:.8f}. Took: {can_take_from_level:.8f}. Acc.Base: {accumulated_base_amount:.8f}")

                if accumulated_base_amount >= amount_base:
                    break 

            if accumulated_base_amount < amount_base:
                self.logger.warning(f"Insufficient depth for {symbol} on {exchange_id} to fill {amount_base:.8f} {side.upper()} within slippage. "
                                  f"Could only fill {accumulated_base_amount:.8f} at acceptable prices.")
                return False

            avg_fill_price = accumulated_quote_cost / accumulated_base_amount if accumulated_base_amount > 0 else Decimal(0)

            if side == 'buy':
                if avg_fill_price > price_limit_buy:
                    self.logger.warning(f"Liquidity check FAIL (BUY): Avg fill price {avg_fill_price:.8f} for {symbol} on {exchange_id} "
                                      f"exceeds target {price_target:.8f} with slippage limit {price_limit_buy:.8f}.")
                    return False
            elif side == 'sell':
                if avg_fill_price < price_limit_sell:
                    self.logger.warning(f"Liquidity check FAIL (SELL): Avg fill price {avg_fill_price:.8f} for {symbol} on {exchange_id} "
                                      f"is below target {price_target:.8f} with slippage limit {price_limit_sell:.8f}.")
                    return False

            self.logger.info(f"Liquidity check PASS for {symbol} on {exchange_id} ({side}). "
                             f"Able to fill {accumulated_base_amount:.8f} at avg price {avg_fill_price:.8f} (target: {price_target:.8f}).")
            return True

        except Exception as e:
            self.logger.error(f"Error during liquidity check for {symbol} on {exchange_id}: {e}", exc_info=True)
            return False

class Scanner:
    def __init__(self, exchanges: Dict[str, ccxt_async.Exchange]):
        self.exchanges = exchanges
        self.logger = logging.getLogger(self.__class__.__name__)
        # ИСПРАВЛЕНО: Атрибут common_pairs должен быть инициализирован здесь
        self.common_pairs: Dict[Tuple[str, str], List[str]] = {}

    @safe_call_wrapper()
    async def initialize(self):
        self.logger.info("Scanner initializing: Loading markets for all exchanges...")
        load_market_tasks = {}
        original_timeouts = {}

        for ex_id, ex_inst in self.exchanges.items():
            if not ex_inst:
                self.logger.warning(f"Scanner: Exchange instance for {ex_id} is None. Skipping market load.")
                continue
            try:
                original_timeouts[ex_id] = getattr(ex_inst, 'timeout', 30000)
                ex_inst.timeout = 90000
                load_market_tasks[ex_id] = asyncio.create_task(ex_inst.load_markets(reload=False))
            except Exception as e:
                self.logger.error(f"Error creating market load task for {ex_id}: {e}")
                if ex_id in original_timeouts and ex_inst:
                    setattr(ex_inst, 'timeout', original_timeouts[ex_id])


        if load_market_tasks:
            results = await asyncio.gather(*load_market_tasks.values(), return_exceptions=True)
            for ex_id, res in zip(load_market_tasks.keys(), results):
                ex_inst_after_load = self.exchanges.get(ex_id)
                if ex_inst_after_load and ex_id in original_timeouts:
                    setattr(ex_inst_after_load, 'timeout', original_timeouts[ex_id])

                if isinstance(res, Exception):
                    self.logger.error(f"Failed to load markets for {ex_id}: {res}")
                elif self.exchanges.get(ex_id) and self.exchanges[ex_id].markets:
                    self.logger.debug(f"Markets loaded successfully for {ex_id} (Count: {len(self.exchanges[ex_id].markets)}).")
                else:
                    self.logger.error(f"Market loading task for {ex_id} completed, but 'markets' attribute is empty or None, or exchange not in self.exchanges.")

        successful_exchange_ids = {ex_id for ex_id, ex in self.exchanges.items() if ex and ex.markets}

        if len(successful_exchange_ids) < 2:
            self.logger.critical(f"Scanner requires at least 2 exchanges with successfully loaded markets to find arbitrage opportunities. Loaded for: {list(successful_exchange_ids)}. Aborting further initialization.")
            return

        self.logger.info(f"Markets loaded for: {list(successful_exchange_ids)}. Finding common, active, non-leveraged {Config.QUOTE_ASSET}-quoted spot pairs...")
        self.common_pairs.clear()

        for ex1_id, ex2_id in itertools.combinations(list(successful_exchange_ids), 2):
            try:
                ex1_inst = self.exchanges[ex1_id]
                ex2_inst = self.exchanges[ex2_id]

                if not ex1_inst.markets or not ex2_inst.markets:
                    self.logger.warning(f"Markets not available for {ex1_id} or {ex2_id} despite earlier check. Skipping pair commonality.")
                    continue

                common_symbols_on_both = set(ex1_inst.markets.keys()).intersection(set(ex2_inst.markets.keys()))
                active_common_spot_pairs = set()
                quote_asset_upper = Config.QUOTE_ASSET.upper()

                for symbol_str in common_symbols_on_both:
                    base_asset_candidate = symbol_str.split('/')[0]
                    if Config.is_leveraged_token(base_asset_candidate):
                        self.logger.debug(f"Scanner init: Filtering out leveraged token '{symbol_str}'.")
                        continue

                    market_details_ex1 = ex1_inst.markets.get(symbol_str, {})
                    market_details_ex2 = ex2_inst.markets.get(symbol_str, {})

                    if symbol_str.endswith(f'/{quote_asset_upper}') and \
                       market_details_ex1.get('active', False) and market_details_ex2.get('active', False) and \
                       market_details_ex1.get('spot', False) and market_details_ex2.get('spot', False):
                        active_common_spot_pairs.add(symbol_str)

                if active_common_spot_pairs:
                    self.common_pairs[(ex1_id, ex2_id)] = sorted(list(active_common_spot_pairs))
                    self.logger.debug(f"Found {len(active_common_spot_pairs)} common, active, non-leveraged {quote_asset_upper} spot pairs for {ex1_id} - {ex2_id}.")
            except Exception as e:
                self.logger.error(f"Error finding common pairs for combination {ex1_id}/{ex2_id}: {e}", exc_info=True)

        total_common_pair_definitions = sum(len(pairs_list) for pairs_list in self.common_pairs.values())
        if total_common_pair_definitions > 0:
            self.logger.info(f"Scanner initialized. Monitoring {total_common_pair_definitions} unique common pair instances across exchange combinations.")
        else:
            self.logger.warning("Scanner initialized, but no common, active, non-leveraged spot pairs found across any exchange combination.")


    @safe_call_wrapper(default_retval=[])
    async def scan_once(self) -> List[ArbitrageOpportunity]:
        self.logger.info("--- Starting new scan cycle ---")
        if not self.exchanges:
            self.logger.error("Scanner: Exchange instances are not set. Cannot scan.")
            return []

        active_ex_ids_with_markets = [ex_id for ex_id, ex in self.exchanges.items() if ex and ex.markets]
        if not self.common_pairs and len(active_ex_ids_with_markets) >= 2:
            self.logger.warning("Scanner: Common pairs not pre-calculated or lost. Re-initializing common pairs...")
            await self.initialize()

        if not self.common_pairs:
            self.logger.error("Scanner: No common pairs available to monitor. Scan cycle aborted.")
            return []

        symbols_to_fetch_per_exchange: Dict[str, Set[str]] = {}
        for (ex1_id, ex2_id), symbols_list in self.common_pairs.items():
            if ex1_id in active_ex_ids_with_markets and ex2_id in active_ex_ids_with_markets:
                for sym in symbols_list:
                    if Config.is_leveraged_token(sym.split('/')[0]):
                        continue
                    symbols_to_fetch_per_exchange.setdefault(ex1_id, set()).add(sym)
                    symbols_to_fetch_per_exchange.setdefault(ex2_id, set()).add(sym)

        if not symbols_to_fetch_per_exchange:
            self.logger.info("Scanner: No symbols identified for fetching tickers in this cycle (e.g. all common pairs involved inactive exchanges).")
            return []

        fetch_ticker_tasks: Dict[str, asyncio.Task] = {}
        for ex_id, symbols_set in symbols_to_fetch_per_exchange.items():
            ex_inst = self.exchanges.get(ex_id)
            if not ex_inst or not symbols_set:
                continue

            if hasattr(ex_inst, 'has') and ex_inst.has.get('fetchTickers'):
                try:
                    fetch_ticker_tasks[ex_id] = asyncio.create_task(ex_inst.fetch_tickers(list(symbols_set)))
                except Exception as e:
                    self.logger.error(f"Scanner: Error creating fetchTickers (plural) task for {ex_id}: {e}")
            else:
                self.logger.warning(f"Scanner: Exchange {ex_id} does not efficiently support fetchTickers (plural). Performance might be impacted if many symbols.")


        all_fetched_tickers: Dict[str, Dict[str, Any]] = {}
        opportunities: List[ArbitrageOpportunity] = []

        if fetch_ticker_tasks:
            task_results = await asyncio.gather(*fetch_ticker_tasks.values(), return_exceptions=True)
            for ex_id, result in zip(fetch_ticker_tasks.keys(), task_results):
                if isinstance(result, Exception):
                    self.logger.warning(f"Scanner: Failed to fetch tickers for {ex_id}: {result}")
                elif result and isinstance(result, dict):
                    all_fetched_tickers[ex_id] = result
                    self.logger.debug(f"Scanner: Successfully fetched {len(result)} tickers for {ex_id}.")
                else:
                    self.logger.warning(f"Scanner: No tickers returned or unexpected format for {ex_id}. Result: {type(result)}")

        if not all_fetched_tickers:
            self.logger.info("Scanner: No tickers were successfully fetched from any exchange in this cycle.")
            return opportunities

        for (ex1_id, ex2_id), symbols_list in self.common_pairs.items():
            if ex1_id not in all_fetched_tickers or ex2_id not in all_fetched_tickers:
                continue

            tickers_ex1 = all_fetched_tickers[ex1_id]
            tickers_ex2 = all_fetched_tickers[ex2_id]

            for symbol_str in symbols_list:
                if Config.is_leveraged_token(symbol_str.split('/')[0]):
                    continue

                ticker1_data = tickers_ex1.get(symbol_str)
                ticker2_data = tickers_ex2.get(symbol_str)

                if not ticker1_data or not ticker2_data:
                    continue

                def get_relevant_price(ticker_dict: Dict, side: str) -> Optional[Decimal]:
                    price_val = None
                    if side == 'ask': price_val = ticker_dict.get('ask')
                    elif side == 'bid': price_val = ticker_dict.get('bid')

                    if price_val is None: price_val = ticker_dict.get('last') or ticker_dict.get('close')

                    try: return Decimal(str(price_val)) if price_val is not None else None
                    except (InvalidOperation, TypeError): return None

                buy_price_ex1 = get_relevant_price(ticker1_data, 'ask')
                sell_price_ex2 = get_relevant_price(ticker2_data, 'bid')

                if buy_price_ex1 and sell_price_ex2 and buy_price_ex1 > 0 and sell_price_ex2 > 0 and buy_price_ex1 < sell_price_ex2:
                    gross_profit_percentage = ((sell_price_ex2 - buy_price_ex1) / buy_price_ex1) * Decimal('100')
                    if Config.MIN_PROFIT_THRESHOLD_GROSS <= gross_profit_percentage <= Config.MAX_PROFIT_THRESHOLD:
                        opportunities.append(ArbitrageOpportunity(
                            buy_exchange_id=ex1_id, sell_exchange_id=ex2_id, symbol=symbol_str,
                            buy_price=buy_price_ex1, sell_price=sell_price_ex2, gross_profit_pct=gross_profit_percentage
                        ))

                buy_price_ex2 = get_relevant_price(ticker2_data, 'ask')
                sell_price_ex1 = get_relevant_price(ticker1_data, 'bid')

                if buy_price_ex2 and sell_price_ex1 and buy_price_ex2 > 0 and sell_price_ex1 > 0 and buy_price_ex2 < sell_price_ex1:
                    gross_profit_percentage = ((sell_price_ex1 - buy_price_ex2) / buy_price_ex2) * Decimal('100')
                    if Config.MIN_PROFIT_THRESHOLD_GROSS <= gross_profit_percentage <= Config.MAX_PROFIT_THRESHOLD:
                        opportunities.append(ArbitrageOpportunity(
                            buy_exchange_id=ex2_id, sell_exchange_id=ex1_id, symbol=symbol_str,
                            buy_price=buy_price_ex2, sell_price=sell_price_ex1, gross_profit_pct=gross_profit_percentage
                        ))

        if opportunities:
            self.logger.info(f"Scan cycle found {len(opportunities)} potential gross arbitrage opportunities meeting thresholds.")
        else:
            self.logger.info("Scan cycle completed. No potential gross arbitrage opportunities found meeting thresholds.")
        return opportunities
    


class Analyzer:
    def __init__(self, exchanges: Dict[str, ccxt_async.Exchange]):
        self.exchanges = exchanges
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stable_candidates: Dict[str, ArbitrageOpportunity] = {}
        self.currencies_cache: Dict[str, Dict[str, Any]] = {}
        self.balance_manager: Optional['BalanceManager'] = None
        self.executor: Optional['Executor'] = None

    def set_balance_manager(self, balance_manager: 'BalanceManager'):
        self.balance_manager = balance_manager

    def set_executor(self, executor: 'Executor'):
        self.executor = executor

    @safe_call_wrapper(default_retval=None)
    async def get_asset_price_in_usdt(self, asset_code: str,
                                      preferred_ref_exchange_id: str = 'binance',
                                      opportunity_context: Optional[ArbitrageOpportunity] = None) -> Optional[Decimal]:
        asset_code_upper = asset_code.upper()
        quote_asset_upper = Config.QUOTE_ASSET.upper()

        if asset_code_upper == quote_asset_upper:
            return Decimal("1.0")

        exchanges_to_try_order: List[Tuple[str, str]] = []

        if opportunity_context:
            if opportunity_context.buy_exchange_id in self.exchanges:
                exchanges_to_try_order.append((opportunity_context.buy_exchange_id, f"opportunity buy_exchange ({opportunity_context.buy_exchange_id})"))
            if opportunity_context.sell_exchange_id in self.exchanges and \
               opportunity_context.sell_exchange_id != opportunity_context.buy_exchange_id:
                exchanges_to_try_order.append((opportunity_context.sell_exchange_id, f"opportunity sell_exchange ({opportunity_context.sell_exchange_id})"))

        if preferred_ref_exchange_id in self.exchanges and \
           preferred_ref_exchange_id not in [ex_id for ex_id, _ in exchanges_to_try_order]:
            exchanges_to_try_order.append((preferred_ref_exchange_id, f"primary reference ({preferred_ref_exchange_id})"))

        tried_ids_set = {ex_id for ex_id, _ in exchanges_to_try_order}
        for ex_id_fallback, ex_inst_fallback in self.exchanges.items():
            if ex_id_fallback not in tried_ids_set and ex_inst_fallback and \
               hasattr(ex_inst_fallback, 'has') and ex_inst_fallback.has.get('fetchTicker') and ex_inst_fallback.markets:
                exchanges_to_try_order.append((ex_id_fallback, f"fallback ({ex_id_fallback})"))

        if not exchanges_to_try_order:
            self.logger.warning(f"No suitable exchanges available to fetch live price for {asset_code_upper}.")
            est_price = Config.ESTIMATED_ASSET_PRICES_USD.get(asset_code_upper)
            if est_price: self.logger.warning(f"Using ESTIMATED_ASSET_PRICES_USD for {asset_code_upper}: ${est_price}"); return est_price
            self.logger.error(f"Failed to get USD price for {asset_code_upper}: No exchanges and no estimate."); return None

        symbol_to_fetch_vs_usdt = f"{asset_code_upper}/{quote_asset_upper}"
        self.logger.debug(f"Attempting to get USD price for {asset_code_upper}. Order of exchanges to try: {[ex_id for ex_id, _ in exchanges_to_try_order]}")

        for ex_id_to_try, source_description in exchanges_to_try_order:
            exchange_instance = self.exchanges.get(ex_id_to_try)
            if not exchange_instance or not exchange_instance.markets:
                continue

            original_timeout = getattr(exchange_instance, 'timeout', 30000)
            exchange_instance.timeout = 60000
            try:
                if symbol_to_fetch_vs_usdt in exchange_instance.markets:
                    self.logger.debug(f"Fetching ticker for {symbol_to_fetch_vs_usdt} on {exchange_instance.id} (Source: {source_description})")
                    ticker = await exchange_instance.fetch_ticker(symbol_to_fetch_vs_usdt)
                    price_value_str = (ticker.get('last') or ticker.get('ask') or ticker.get('bid') or ticker.get('close')) if ticker else None

                    if price_value_str is not None:
                        price_decimal = Decimal(str(price_value_str))
                        if price_decimal > 0:
                            self.logger.info(f"Live price for {symbol_to_fetch_vs_usdt} obtained from {exchange_instance.id} ({source_description}): {price_decimal}")
                            exchange_instance.timeout = original_timeout; return price_decimal
                        else:
                            self.logger.debug(f"Fetched zero or negative price ({price_decimal}) for {symbol_to_fetch_vs_usdt} from {exchange_instance.id}.")
                    else:
                        self.logger.debug(f"No usable price (last/ask/bid/close) in ticker for {symbol_to_fetch_vs_usdt} from {exchange_instance.id}.")
                else:
                    self.logger.debug(f"Market {symbol_to_fetch_vs_usdt} not listed on {exchange_instance.id} ({source_description}).")
            except (ccxt_async.NetworkError, ccxt_async.RequestTimeout, ccxt_async.ExchangeError) as e_ccxt:
                self.logger.warning(f"CCXT error fetching price for {asset_code_upper} on {exchange_instance.id} ({source_description}): {e_ccxt}")
            except (InvalidOperation, TypeError) as e_parse:
                self.logger.warning(f"Error parsing price for {asset_code_upper} on {exchange_instance.id} ({source_description}): {e_parse}")
            except Exception as e_unexp:
                self.logger.warning(f"Unexpected error fetching price for {asset_code_upper} on {exchange_instance.id} ({source_description}): {e_unexp}", exc_info=True)
            finally:
                if exchange_instance:
                    exchange_instance.timeout = original_timeout

        estimated_price = Config.ESTIMATED_ASSET_PRICES_USD.get(asset_code_upper)
        if estimated_price:
            self.logger.warning(f"Failed to fetch live price for {asset_code_upper} from all sources. Using ESTIMATED_ASSET_PRICES_USD: ${estimated_price}.")
            return estimated_price

        self.logger.error(f"Failed to get any USD price (live or estimated) for {asset_code_upper}.")
        return None

    @safe_call_wrapper(default_retval=None)
    async def analyze_and_select_best(self, opportunities: List[ArbitrageOpportunity]) -> Optional[ArbitrageOpportunity]:
        if not opportunities:
            self.logger.debug("Analyzer: No gross opportunities provided to analyze_and_select_best.")
            return None

        valid_opportunities_pre_stability = []
        for opp in opportunities:
            if Config.is_leveraged_token(opp.base_asset):
                self.logger.debug(f"Analyzer: Filtering out leveraged token opportunity: {opp.get_unique_id()}")
                continue
            
            if (opp.buy_exchange_id.lower(), opp.base_asset.upper()) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
               (opp.sell_exchange_id.lower(), opp.base_asset.upper()) in Config.ASSET_UNAVAILABLE_BLACKLIST:
                self.logger.debug(f"Analyzer: Filtering out opportunity {opp.get_unique_id()} due to general ASSET_UNAVAILABLE_BLACKLIST.")
                continue

            valid_opportunities_pre_stability.append(opp)

        if not valid_opportunities_pre_stability:
            self.logger.info("Analyzer: All provided opportunities were leveraged or generally blacklisted. No valid opportunities to analyze.")
            return None

        self._update_stability_counts(valid_opportunities_pre_stability)

        stable_opportunities_gross = [
            opp for opp in self.stable_candidates.values()
            if opp.is_stable
        ]

        if not stable_opportunities_gross:
            self.logger.info("Analyzer: No opportunities met the STABILITY_CYCLES requirement.")
            return None
        
        self.logger.info(f"Analyzer: Found {len(stable_opportunities_gross)} stable gross opportunities. Enriching top {Config.TOP_N_OPPORTUNITIES} by gross profit...")
        stable_opportunities_gross.sort(key=lambda x: x.gross_profit_pct, reverse=True)
        
        enriched_stable_opportunities = await self._enrich_opportunities_with_fees(
            stable_opportunities_gross[:Config.TOP_N_OPPORTUNITIES]
        )

        final_candidate_opportunities: List[ArbitrageOpportunity] = []
        for opp in enriched_stable_opportunities:
            if not opp.potential_networks:
                self.logger.debug(f"Opp {opp.get_unique_id()} has no potential_networks after enrichment. Skipping whitelist/blacklist check.")
                continue

            selected_network_for_this_opp = False
            for net_detail in opp.potential_networks:
                current_path_tuple = (
                    opp.base_asset.upper(),
                    opp.buy_exchange_id.lower(),
                    opp.sell_exchange_id.lower(),
                    net_detail['normalized_name'].upper()
                )

                if current_path_tuple in Config.ARB_PATH_BLACKLIST:
                    self.logger.debug(f"Path {current_path_tuple} for opp {opp.get_unique_id()} is specifically blacklisted. Checking next network option for this opp.")
                    continue

                is_this_network_whitelisted = current_path_tuple in Config.ARB_WHITELIST
                
                if Config.ENFORCE_WHITELIST_ONLY and not is_this_network_whitelisted:
                    self.logger.debug(f"Path {current_path_tuple} for opp {opp.get_unique_id()} is not whitelisted (enforcement on). Checking next network option.")
                    continue
                
                initial_best_net_detail_in_enrich = opp.chosen_network_details
                
                if net_detail['normalized_name'] != (initial_best_net_detail_in_enrich.get('normalized_name') if initial_best_net_detail_in_enrich else None):
                    self.logger.debug(f"Re-evaluating net profit for {opp.get_unique_id()} with network {net_detail['normalized_name']} (was previously {initial_best_net_detail_in_enrich.get('normalized_name') if initial_best_net_detail_in_enrich else 'None'}) due to whitelist/blacklist selection.")
                    new_withdrawal_fee_usd = None
                    native_fee = net_detail['fee_native']
                    fee_currency = net_detail.get('fee_currency', opp.base_asset)

                    if fee_currency.upper() == opp.base_asset.upper():
                        new_withdrawal_fee_usd = native_fee * opp.buy_price
                    elif fee_currency.upper() == Config.QUOTE_ASSET.upper():
                        new_withdrawal_fee_usd = native_fee
                    else:
                        fee_curr_price_usdt = await self.get_asset_price_in_usdt(fee_currency, preferred_ref_exchange_id=opp.buy_exchange_id, opportunity_context=opp)
                        if fee_curr_price_usdt and fee_curr_price_usdt > 0:
                            new_withdrawal_fee_usd = native_fee * fee_curr_price_usdt
                        else:
                            self.logger.warning(f"Could not get price for fee currency {fee_currency} for {opp.get_unique_id()}. Using default high fee.")
                            new_withdrawal_fee_usd = Config.ESTIMATED_WITHDRAWAL_FEES_USD.get('DEFAULT_ASSET_FEE')
                    
                    opp.withdrawal_fee_usd = new_withdrawal_fee_usd
                    if new_withdrawal_fee_usd is not None and Config.TRADE_AMOUNT_USD > 0:
                        withdrawal_fee_pct_of_trade = (new_withdrawal_fee_usd / Config.TRADE_AMOUNT_USD) * Decimal('100')
                        opp.net_profit_pct = opp.gross_profit_pct - (opp.buy_fee_pct or 0) - (opp.sell_fee_pct or 0) - withdrawal_fee_pct_of_trade
                    else:
                        opp.net_profit_pct = None
                
                opp.chosen_network = net_detail['normalized_name']
                opp.chosen_network_details = net_detail

                final_candidate_opportunities.append(opp)
                selected_network_for_this_opp = True
                break

            if not selected_network_for_this_opp:
                 self.logger.debug(f"Opp {opp.get_unique_id()} had no valid (whitelisted/not_blacklisted) networks in its potential_networks list after filtering. Skipping this opp.")
        
        if not final_candidate_opportunities:
            self.logger.info("Analyzer: No opportunities remained after whitelist/blacklist filtering.")
            return None

        net_profitable_opportunities = [
            opp for opp in final_candidate_opportunities
            if opp.net_profit_pct is not None and opp.net_profit_pct >= Config.MIN_PROFIT_THRESHOLD_NET
        ]

        if not net_profitable_opportunities:
            self.logger.info("Analyzer: No opportunities remained net profitable after whitelist/blacklist and fee calculation.")
            return None

        net_profitable_opportunities.sort(key=lambda x: x.net_profit_pct, reverse=True)

        best_opportunity_final: Optional[ArbitrageOpportunity] = None
        for potential_best_opp in net_profitable_opportunities:
            if not potential_best_opp.chosen_network_details:
                 self.logger.warning(f"Opportunity {potential_best_opp.get_unique_id()} has no chosen_network_details. Skipping liquidity check.")
                 continue

            if self.executor and hasattr(self.executor, '_check_liquidity_for_trade_leg'):
                self.logger.debug(f"Performing final liquidity check for selected opportunity: {potential_best_opp.get_unique_id()}")
                
                buy_liquid = await self.executor._check_liquidity_for_trade_leg(
                    potential_best_opp.buy_exchange_id, potential_best_opp.symbol, 'buy',
                    Config.TRADE_AMOUNT_USD, potential_best_opp.buy_price, is_base_amount=False
                )
                
                approx_base_amount = Config.TRADE_AMOUNT_USD / potential_best_opp.buy_price if potential_best_opp.buy_price > 0 else Decimal(0)
                sell_liquid = False
                if approx_base_amount > 0:
                    sell_liquid = await self.executor._check_liquidity_for_trade_leg(
                        potential_best_opp.sell_exchange_id, potential_best_opp.symbol, 'sell',
                        approx_base_amount, potential_best_opp.sell_price, is_base_amount=True
                    )
                
                potential_best_opp.is_liquid_enough_for_trade = buy_liquid and sell_liquid
                if potential_best_opp.is_liquid_enough_for_trade:
                    best_opportunity_final = potential_best_opp
                    break
                else:
                    self.logger.warning(f"Opportunity {potential_best_opp.get_unique_id()} failed final liquidity check. Buy liquid: {buy_liquid}, Sell liquid: {sell_liquid}. Trying next.")
            else:
                self.logger.warning("Executor or _check_liquidity_for_trade_leg not available. Selecting best opportunity without final liquidity check.")
                potential_best_opp.is_liquid_enough_for_trade = True
                best_opportunity_final = potential_best_opp
                break
        
        if not best_opportunity_final:
            self.logger.info("Analyzer: No net profitable opportunities passed the final liquidity check (if performed).")
            return None

        self.logger.info(f"🏆 Best Full Arbitrage Opportunity Selected: {best_opportunity_final.get_unique_id()} | Symbol: {best_opportunity_final.symbol} | "
                         f"Gross Profit: {best_opportunity_final.gross_profit_pct:.3f}% -> Net Profit: {best_opportunity_final.net_profit_pct:.3f}% "
                         f"(Buy: {best_opportunity_final.buy_exchange_id} @ {best_opportunity_final.buy_price:.8f}, Sell: {best_opportunity_final.sell_exchange_id} @ {best_opportunity_final.sell_price:.8f}) "
                         f"Withdrawal Fee (est.): ${best_opportunity_final.withdrawal_fee_usd if best_opportunity_final.withdrawal_fee_usd is not None else 'N/A':.2f} via {best_opportunity_final.chosen_network or 'N/A'} "
                         f"Liquid: {best_opportunity_final.is_liquid_enough_for_trade}")

        if best_opportunity_final.get_unique_id() in self.stable_candidates:
            del self.stable_candidates[best_opportunity_final.get_unique_id()]
            self.logger.debug(f"Removed chosen opportunity {best_opportunity_final.get_unique_id()} from stable_candidates.")

        return best_opportunity_final

    def _update_stability_counts(self, current_scan_opportunities: List[ArbitrageOpportunity]):
        current_opportunity_ids_set = {opp.get_unique_id() for opp in current_scan_opportunities}

        for opp in current_scan_opportunities:
            uid = opp.get_unique_id()
            if uid in self.stable_candidates:
                self.stable_candidates[uid].stability_count += 1
                self.stable_candidates[uid].buy_price = opp.buy_price
                self.stable_candidates[uid].sell_price = opp.sell_price
                self.stable_candidates[uid].gross_profit_pct = opp.gross_profit_pct
            else:
                self.stable_candidates[uid] = opp
                self.stable_candidates[uid].stability_count = 1

            if self.stable_candidates[uid].stability_count >= Config.STABILITY_CYCLES:
                self.stable_candidates[uid].is_stable = True
        
        stale_candidate_uids = [uid for uid in self.stable_candidates if uid not in current_opportunity_ids_set]
        for uid_to_remove in stale_candidate_uids:
            removed_candidate = self.stable_candidates.pop(uid_to_remove, None)
            if removed_candidate:
                 self.logger.debug(f"Removed stale/disappeared candidate: {removed_candidate.get_unique_id()} (was stable: {removed_candidate.is_stable}, count: {removed_candidate.stability_count})")
        if stale_candidate_uids:
            self.logger.debug(f"Removed {len(stale_candidate_uids)} stale/disappeared candidates from stability tracking.")

    @safe_call_wrapper(default_retval=[])
    async def _enrich_opportunities_with_fees(self, opportunities: List[ArbitrageOpportunity]) -> List[ArbitrageOpportunity]:
        self.logger.debug(f"Enriching {len(opportunities)} opportunities with detailed fee data (including base asset withdrawal)...")
        enriched_list: List[ArbitrageOpportunity] = []

        await self._cache_all_currencies_info_for_enrichment(opportunities)

        for opp in opportunities:
            self.logger.debug(f"Enriching details for opportunity: {opp.get_unique_id()}")
            try:
                buy_ex_inst = self.exchanges.get(opp.buy_exchange_id)
                sell_ex_inst = self.exchanges.get(opp.sell_exchange_id)

                if not buy_ex_inst or not sell_ex_inst or \
                   not buy_ex_inst.markets or opp.symbol not in buy_ex_inst.markets or \
                   not sell_ex_inst.markets or opp.symbol not in sell_ex_inst.markets:
                    self.logger.warning(f"Market data or exchange instance missing for {opp.get_unique_id()}. Cannot enrich. Skipping.")
                    opp.net_profit_pct = None
                    enriched_list.append(opp)
                    continue
                
                buy_market_details = buy_ex_inst.markets[opp.symbol]
                sell_market_details = sell_ex_inst.markets[opp.symbol]

                default_taker_fee_rate = Decimal('0.001')

                buy_taker_fee_rate_str = buy_market_details.get('taker', default_taker_fee_rate)
                sell_taker_fee_rate_str = sell_market_details.get('taker', default_taker_fee_rate)

                try:
                    opp.buy_fee_pct = Decimal(str(buy_taker_fee_rate_str)) * Decimal('100')
                    opp.sell_fee_pct = Decimal(str(sell_taker_fee_rate_str)) * Decimal('100')
                except (InvalidOperation, TypeError) as e_fee_parse:
                    self.logger.warning(f"Could not parse taker fee for {opp.get_unique_id()}: {e_fee_parse}. Using default 0.1%. Buy raw: '{buy_taker_fee_rate_str}', Sell raw: '{sell_taker_fee_rate_str}'")
                    opp.buy_fee_pct = default_taker_fee_rate * Decimal('100')
                    opp.sell_fee_pct = default_taker_fee_rate * Decimal('100')

                approx_base_to_withdraw = Config.TRADE_AMOUNT_USD / opp.buy_price if opp.buy_price > 0 else Decimal(0)
                
                potential_networks_list = await self._select_optimal_network_for_transfer(
                    asset_code=opp.base_asset,
                    from_exchange_id=opp.buy_exchange_id,
                    to_exchange_id=opp.sell_exchange_id,
                    amount_native_to_withdraw_optional=approx_base_to_withdraw
                )
                opp.potential_networks = potential_networks_list if potential_networks_list else []

                opp.withdrawal_fee_usd = None
                opp.chosen_network = None
                opp.chosen_network_details = None

                if opp.potential_networks:
                    best_network_for_fee_calc = opp.potential_networks[0]
                    opp.chosen_network = best_network_for_fee_calc['normalized_name']
                    opp.chosen_network_details = best_network_for_fee_calc

                    native_fee = best_network_for_fee_calc['fee_native']
                    fee_currency = best_network_for_fee_calc.get('fee_currency', opp.base_asset)

                    if fee_currency.upper() == opp.base_asset.upper():
                        opp.withdrawal_fee_usd = native_fee * opp.buy_price
                    elif fee_currency.upper() == Config.QUOTE_ASSET.upper():
                        opp.withdrawal_fee_usd = native_fee
                    else:
                        fee_curr_price_usdt = await self.get_asset_price_in_usdt(fee_currency, preferred_ref_exchange_id=opp.buy_exchange_id, opportunity_context=opp)
                        if fee_curr_price_usdt and fee_curr_price_usdt > 0:
                            opp.withdrawal_fee_usd = native_fee * fee_curr_price_usdt
                        else:
                            self.logger.warning(f"Could not get price for fee currency {fee_currency} to calculate withdrawal fee in USD for {opp.get_unique_id()}. Estimating high.")
                            opp.withdrawal_fee_usd = Config.ESTIMATED_WITHDRAWAL_FEES_USD.get('DEFAULT_ASSET_FEE')
                else:
                    self.logger.warning(f"No suitable networks found for withdrawing {opp.base_asset} from {opp.buy_exchange_id} to {opp.sell_exchange_id}. Using default high withdrawal fee estimate.")
                    asset_default_fees = Config.ESTIMATED_WITHDRAWAL_FEES_USD.get(opp.base_asset.upper(), {})
                    opp.withdrawal_fee_usd = asset_default_fees.get('DEFAULT', Config.ESTIMATED_WITHDRAWAL_FEES_USD['DEFAULT_ASSET_FEE'])

                if opp.withdrawal_fee_usd is not None and Config.TRADE_AMOUNT_USD > 0:
                    withdrawal_fee_pct_of_trade = (opp.withdrawal_fee_usd / Config.TRADE_AMOUNT_USD) * Decimal('100')
                    
                    opp.net_profit_pct = opp.gross_profit_pct - \
                                         (opp.buy_fee_pct or 0) - \
                                         (opp.sell_fee_pct or 0) - \
                                         withdrawal_fee_pct_of_trade
                    
                    self.logger.info(f"Enriched (Full Arbitrage): {opp.get_unique_id()} | "
                                     f"Gross: {opp.gross_profit_pct:.3f}%, BuyFee: {opp.buy_fee_pct or 0:.3f}%, SellFee: {opp.sell_fee_pct or 0:.3f}%, "
                                     f"WithdrawFee ({opp.base_asset} via {opp.chosen_network or 'N/A'}): ~${opp.withdrawal_fee_usd:.2f} ({withdrawal_fee_pct_of_trade:.3f}%) "
                                     f"-> Net Profit Pct: {opp.net_profit_pct:.3f}%")
                else:
                    opp.net_profit_pct = None
                    self.logger.warning(f"Net profit not calculated for {opp.get_unique_id()} due to missing withdrawal fee calculation or zero trade amount.")
                
                enriched_list.append(opp)

            except Exception as e_enrich:
                self.logger.error(f"Error enriching opportunity {opp.get_unique_id()}: {e_enrich}", exc_info=True)
                opp.net_profit_pct = None
                enriched_list.append(opp)
        return enriched_list

    @safe_call_wrapper(default_retval=None)
    async def _cache_all_currencies_info_for_enrichment(self, opportunities: List[ArbitrageOpportunity]):
        exchanges_needing_currency_fetch = set()
        for ex_id in self.exchanges.keys():
             if ex_id not in self.currencies_cache or not self.currencies_cache.get(ex_id):
                exchanges_needing_currency_fetch.add(ex_id)

        for opp in opportunities:
            if opp.buy_exchange_id not in self.currencies_cache and opp.buy_exchange_id in self.exchanges:
                exchanges_needing_currency_fetch.add(opp.buy_exchange_id)
            if opp.sell_exchange_id not in self.currencies_cache and opp.sell_exchange_id in self.exchanges:
                 exchanges_needing_currency_fetch.add(opp.sell_exchange_id)


        if not exchanges_needing_currency_fetch:
            self.logger.debug("All required currency information for enrichment/network selection appears to be cached.")
            return

        self.logger.info(f"Fetching/refreshing currency information from CCXT for exchanges: {list(exchanges_needing_currency_fetch)}")
        currency_fetch_tasks: Dict[str, asyncio.Task] = {}
        original_timeouts_curr = {}

        for ex_id in exchanges_needing_currency_fetch:
            ex_inst = self.exchanges.get(ex_id)
            if ex_inst and hasattr(ex_inst, 'has') and ex_inst.has.get('fetchCurrencies'):
                original_timeouts_curr[ex_id] = getattr(ex_inst, 'timeout', 30000)
                ex_inst.timeout = 90000
                self.logger.debug(f"Fetching currencies for {ex_id} with timeout {ex_inst.timeout}ms")
                currency_fetch_tasks[ex_id] = asyncio.create_task(ex_inst.fetch_currencies())
            else:
                self.logger.warning(f"Exchange {ex_id} does not support fetchCurrencies or instance not available. Currency cache for it will be empty or unchanged.")
                self.currencies_cache[ex_id] = self.currencies_cache.get(ex_id, {})

        if currency_fetch_tasks:
            task_keys_ordered = list(currency_fetch_tasks.keys())
            results = await asyncio.gather(*currency_fetch_tasks.values(), return_exceptions=True)
            for ex_id, result in zip(task_keys_ordered, results):
                ex_inst_after_fetch = self.exchanges.get(ex_id)
                if ex_inst_after_fetch and ex_id in original_timeouts_curr:
                    setattr(ex_inst_after_fetch, 'timeout', original_timeouts_curr[ex_id])

                if not isinstance(result, Exception) and result and isinstance(result, dict):
                    self.currencies_cache[ex_id] = result
                    self.logger.info(f"Successfully cached/updated currency information for {ex_id} (Total currencies: {len(result)}).")
                elif isinstance(result, Exception):
                    self.logger.error(f"Failed to fetch currencies for {ex_id}: {result}. Existing cache (if any) retained.")
                    self.currencies_cache[ex_id] = self.currencies_cache.get(ex_id, {})
                else:
                    self.logger.warning(f"No currency data returned or unexpected format from {ex_id}. Result type: {type(result)}. Using existing/empty cache.")
                    self.currencies_cache[ex_id] = self.currencies_cache.get(ex_id, {})
        self.logger.debug("Finished caching/refreshing currency information from CCXT.")

    @safe_call_wrapper(default_retval=[])
    async def _select_optimal_network_for_transfer(self,
                                               asset_code: str,
                                               from_exchange_id: str,
                                               to_exchange_id: str,
                                               amount_native_to_withdraw_optional: Optional[Decimal] = None
                                               ) -> List[Dict[str, Any]]:
        self.logger.debug(f"Selecting optimal transfer network(s) for {asset_code} from {from_exchange_id} to {to_exchange_id}"
                          f"{f' (amount: {amount_native_to_withdraw_optional})' if amount_native_to_withdraw_optional else ''}")
        asset_code_upper = asset_code.upper()

        # --- ИСПРАВЛЕНО: Применение нового правила TOKEN_NETWORK_RESTRICTIONS ---
        restriction_key = (from_exchange_id.lower(), asset_code_upper)
        allowed_networks_for_path = Config.TOKEN_NETWORK_RESTRICTIONS.get(restriction_key)
        if allowed_networks_for_path:
            self.logger.info(f"Applying network restriction for {asset_code_upper} from {from_exchange_id}: Only networks {allowed_networks_for_path} are allowed.")

        if (from_exchange_id.lower(), asset_code_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
           (to_exchange_id.lower(), asset_code_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST:
            self.logger.warning(f"Asset {asset_code_upper} transfer involving {from_exchange_id} or {to_exchange_id} is generally blacklisted. No networks will be selected.")
            return []

        potential_from_networks_details: List[Dict[str, Any]] = []
        
        from_ex_id_lower = from_exchange_id.lower()
        from_asset_direct_data = Config.LOADED_EXCHANGE_FEES_DATA.get(from_ex_id_lower, {}).get(asset_code_upper, {})
        from_networks_direct = from_asset_direct_data.get('networks', {}) if isinstance(from_asset_direct_data, dict) else {}

        for norm_net_name_direct, direct_net_info in from_networks_direct.items():
            if not isinstance(direct_net_info, dict) or direct_net_info.get('fee') is None:
                continue
            if not direct_net_info.get('can_withdraw', True):
                self.logger.debug(f"JSON Network (FROM): '{direct_net_info.get('original_name_from_file', norm_net_name_direct)}' on {from_exchange_id} for {asset_code_upper} is marked 'can_withdraw: false'. Skipping.")
                continue

            # --- ИСПРАВЛЕНО: Проверка на соответствие правилу TOKEN_NETWORK_RESTRICTIONS ---
            if allowed_networks_for_path and norm_net_name_direct not in allowed_networks_for_path:
                self.logger.debug(f"Skipping network '{norm_net_name_direct}' from direct config for {asset_code_upper} from {from_exchange_id} due to restriction. Allowed: {allowed_networks_for_path}")
                continue

            original_name_from_direct_data = direct_net_info.get('original_name_from_file', norm_net_name_direct)
            min_wd_val = direct_net_info.get('min_withdraw', Decimal('0'))
            try:
                fee_dec = Decimal(str(direct_net_info['fee']))
                min_wd_dec = Decimal(str(min_wd_val)) if min_wd_val is not None else Decimal('0')
            except (InvalidOperation, TypeError) as e_parse:
                self.logger.warning(f"Error parsing fee/min_withdrawal for direct_data network '{original_name_from_direct_data}' (Asset: {asset_code_upper}, Ex: {from_exchange_id}): {e_parse}. Skipping.")
                continue

            potential_from_networks_details.append({
                'withdraw_network_code_on_from_ex': original_name_from_direct_data,
                'normalized_name': norm_net_name_direct,
                'fee_native': fee_dec,
                'fee_currency': asset_code_upper,
                'min_withdrawal_native': min_wd_dec,
                'source_of_fee': "DIRECT_CONFIG_DATA",
                'can_withdraw': True
            })
            self.logger.debug(f"Added network from DIRECT_CONFIG_DATA (FROM): '{original_name_from_direct_data}' (Normalized: {norm_net_name_direct}) for {asset_code_upper} on {from_exchange_id}.")

        if from_exchange_id not in self.currencies_cache or not self.currencies_cache.get(from_exchange_id):
            await self._cache_all_currencies_info_for_enrichment([])

        from_ex_ccxt_currencies = self.currencies_cache.get(from_exchange_id, {})
        from_asset_ccxt_details = from_ex_ccxt_currencies.get(asset_code_upper, {}) if isinstance(from_ex_ccxt_currencies, dict) else {}
        from_networks_ccxt = from_asset_ccxt_details.get('networks', {}) if isinstance(from_asset_ccxt_details, dict) else {}
        
        for ccxt_net_original_name, ccxt_net_info in from_networks_ccxt.items():
            if not isinstance(ccxt_net_info, dict): continue
            
            normalized_ccxt_net_name = Config.normalize_network_name_for_config(ccxt_net_original_name)
            
            # --- ИСПРАВЛЕНО: Проверка на соответствие правилу TOKEN_NETWORK_RESTRICTIONS ---
            if allowed_networks_for_path and normalized_ccxt_net_name not in allowed_networks_for_path:
                self.logger.debug(f"Skipping network '{normalized_ccxt_net_name}' from CCXT for {asset_code_upper} from {from_exchange_id} due to restriction. Allowed: {allowed_networks_for_path}")
                continue

            if any(pfn['normalized_name'] == normalized_ccxt_net_name for pfn in potential_from_networks_details):
                self.logger.debug(f"Network {normalized_ccxt_net_name} (from CCXT original: {ccxt_net_original_name}) on {from_exchange_id} already covered by direct config data. Skipping CCXT version for FROM_EX.")
                continue

            is_ccxt_net_active = ccxt_net_info.get('active', True)
            is_ccxt_net_withdrawable = ccxt_net_info.get('withdraw', True)
            if not (is_ccxt_net_active and is_ccxt_net_withdrawable):
                self.logger.debug(f"CCXT Network (FROM): '{ccxt_net_original_name}' on {from_exchange_id} for {asset_code_upper} is inactive or not withdrawable. Skipping.")
                continue

            fee_native_val = ccxt_net_info.get('fee')
            fee_currency_code = ccxt_net_info.get('feeCurrency', asset_code_upper)
            min_withdrawal_native_val = ccxt_net_info.get('limits', {}).get('withdraw', {}).get('min')
            
            if fee_native_val is None:
                self.logger.debug(f"CCXT Network (FROM): '{ccxt_net_original_name}' on {from_exchange_id} for {asset_code_upper} has no fee information. Skipping.")
                continue
            try:
                fee_decimal = Decimal(str(fee_native_val))
                min_withdrawal_decimal = Decimal(str(min_withdrawal_native_val)) if min_withdrawal_native_val is not None else Decimal(0)
            except (InvalidOperation, TypeError) as e_parse:
                self.logger.warning(f"Error parsing fee/min_withdrawal for CCXT network '{ccxt_net_original_name}' (Asset: {asset_code_upper}, Ex: {from_exchange_id}): {e_parse}. Skipping.")
                continue
            
            potential_from_networks_details.append({
                'withdraw_network_code_on_from_ex': ccxt_net_original_name,
                'normalized_name': normalized_ccxt_net_name,
                'fee_native': fee_decimal,
                'fee_currency': fee_currency_code.upper(),
                'min_withdrawal_native': min_withdrawal_decimal,
                'source_of_fee': "CCXT_API_DATA",
                'can_withdraw': True
            })
            self.logger.debug(f"Added network from CCXT_API_DATA (FROM): '{ccxt_net_original_name}' (Normalized: {normalized_ccxt_net_name}) for {asset_code_upper} on {from_exchange_id}.")
        
        self.logger.debug(f"[{from_exchange_id}->{to_exchange_id}|{asset_code_upper}] Found {len(potential_from_networks_details)} potential WITHDRAWAL networks on {from_exchange_id}: "
                          f"{[d['normalized_name'] for d in potential_from_networks_details]}")

        if not potential_from_networks_details:
            self.logger.warning(f"No networks found for withdrawing {asset_code_upper} from {from_exchange_id} (or all are disabled/blacklisted). Cannot select optimal network."); return []

        active_deposit_normalized_nets_on_to_ex: Dict[str, str] = {}
        
        to_ex_id_lower = to_exchange_id.lower()
        to_asset_direct_data = Config.LOADED_EXCHANGE_FEES_DATA.get(to_ex_id_lower, {}).get(asset_code_upper, {})
        to_networks_direct_deposit = to_asset_direct_data.get('networks', {}) if isinstance(to_asset_direct_data, dict) else {}

        for norm_net_name_direct_to, direct_to_info in to_networks_direct_deposit.items():
            if isinstance(direct_to_info, dict) and direct_to_info.get('can_deposit', True):
                if norm_net_name_direct_to not in active_deposit_normalized_nets_on_to_ex:
                    active_deposit_normalized_nets_on_to_ex[norm_net_name_direct_to] = direct_to_info.get('original_name_from_file', norm_net_name_direct_to)
                    self.logger.debug(f"Found deposit-enabled network from DIRECT_CONFIG_DATA (TO): '{direct_to_info.get('original_name_from_file', norm_net_name_direct_to)}' (Normalized: {norm_net_name_direct_to}) for {asset_code_upper} on {to_exchange_id}.")
            elif isinstance(direct_to_info, dict) and not direct_to_info.get('can_deposit', True):
                 self.logger.debug(f"JSON Network (TO): '{direct_to_info.get('original_name_from_file', norm_net_name_direct_to)}' on {to_exchange_id} for {asset_code_upper} is marked 'can_deposit: false'. Skipping.")

        if to_exchange_id not in self.currencies_cache or not self.currencies_cache.get(to_exchange_id):
            await self._cache_all_currencies_info_for_enrichment([])

        to_ex_ccxt_currencies = self.currencies_cache.get(to_exchange_id, {})
        to_asset_ccxt_details = to_ex_ccxt_currencies.get(asset_code_upper, {}) if isinstance(to_ex_ccxt_currencies, dict) else {}
        to_networks_ccxt = to_asset_ccxt_details.get('networks', {}) if isinstance(to_asset_ccxt_details, dict) else {}

        for to_net_orig_ccxt, to_net_info_ccxt in to_networks_ccxt.items():
            if isinstance(to_net_info_ccxt, dict) and \
               to_net_info_ccxt.get('active', True) and \
               to_net_info_ccxt.get('deposit', True):
                norm_name_to = Config.normalize_network_name_for_config(to_net_orig_ccxt)
                if norm_name_to not in active_deposit_normalized_nets_on_to_ex:
                    active_deposit_normalized_nets_on_to_ex[norm_name_to] = to_net_orig_ccxt
                    self.logger.debug(f"Found deposit-enabled network from CCXT_API_DATA (TO): '{to_net_orig_ccxt}' (Normalized: {norm_name_to}) for {asset_code_upper} on {to_exchange_id}.")
            elif isinstance(to_net_info_ccxt, dict) and not (to_net_info_ccxt.get('active', True) and to_net_info_ccxt.get('deposit', True)):
                 self.logger.debug(f"CCXT Network (TO): '{to_net_orig_ccxt}' on {to_exchange_id} for {asset_code_upper} is inactive or not depositable. Skipping.")


        self.logger.debug(f"[{from_exchange_id}->{to_exchange_id}|{asset_code_upper}] Found {len(active_deposit_normalized_nets_on_to_ex)} active DEPOSIT networks on {to_exchange_id}: "
                          f"{list(active_deposit_normalized_nets_on_to_ex.keys())}")

        if not active_deposit_normalized_nets_on_to_ex:
            self.logger.warning(f"No active networks found for depositing {asset_code_upper} to {to_exchange_id}. Cannot find transfer path.")
            return []
        
        final_candidate_networks: List[Dict[str, Any]] = []
        for from_net_detail in potential_from_networks_details:
            normalized_name_to_match = from_net_detail['normalized_name']

            if normalized_name_to_match in active_deposit_normalized_nets_on_to_ex and \
               normalized_name_to_match != 'UNKNOWN_NETWORK':

                if amount_native_to_withdraw_optional is not None and \
                   from_net_detail['min_withdrawal_native'] > 0 and \
                   amount_native_to_withdraw_optional < from_net_detail['min_withdrawal_native']:
                    self.logger.debug(f"  Skipping network {normalized_name_to_match} for {asset_code_upper} transfer: "
                                      f"Amount {amount_native_to_withdraw_optional} < Min Withdrawal {from_net_detail['min_withdrawal_native']}")
                    continue

                deposit_net_code_on_to_ex = active_deposit_normalized_nets_on_to_ex.get(normalized_name_to_match)
                if not deposit_net_code_on_to_ex:
                    self.logger.warning(f"Internal inconsistency: Could not find API deposit code for normalized network '{normalized_name_to_match}' on {to_exchange_id}. Skipping this path for safety.")
                    continue
                
                final_candidate_networks.append({
                    'withdraw_network_code_on_from_ex': from_net_detail['withdraw_network_code_on_from_ex'],
                    'deposit_network_code_on_to_ex': deposit_net_code_on_to_ex,
                    'normalized_name': normalized_name_to_match,
                    'fee_native': from_net_detail['fee_native'],
                    'fee_currency': from_net_detail['fee_currency'],
                    'min_withdrawal_native': from_net_detail['min_withdrawal_native'],
                    'source_of_fee': from_net_detail['source_of_fee'],
                    'priority_score_token': Config.get_network_priority_score(normalized_name_to_match, asset_code_upper),
                    'priority_score_general': Config.get_network_priority_score(normalized_name_to_match)
                })
                self.logger.debug(f"  Found compatible and ENABLED transfer path for {asset_code_upper}: "
                                  f"From Ex: {from_exchange_id} (Withdraw Net Code: {from_net_detail['withdraw_network_code_on_from_ex']}) -> "
                                  f"To Ex: {to_exchange_id} (Deposit Net Code for fetchDepositAddress: {deposit_net_code_on_to_ex}). "
                                  f"Normalized: {normalized_name_to_match}, Fee: {from_net_detail['fee_native']} {from_net_detail['fee_currency']}")

        if not final_candidate_networks:
            self.logger.warning(f"No common, active, and enabled (withdraw & deposit) networks found for transferring {asset_code_upper} from {from_exchange_id} to {to_exchange_id} after detailed check, or amount too low for all.")
            return []

        unique_fee_currencies = set(net.get('fee_currency', asset_code_upper) for net in final_candidate_networks)
        fee_currency_prices_in_usd: Dict[str, Optional[Decimal]] = {}
        for fee_curr_code in unique_fee_currencies:
            price_in_usdt = await self.get_asset_price_in_usdt(fee_curr_code, preferred_ref_exchange_id=from_exchange_id)
            if price_in_usdt and price_in_usdt > 0:
                 fee_currency_prices_in_usd[fee_curr_code.upper()] = price_in_usdt
            else:
                estimated_fee_curr_price = Config.ESTIMATED_ASSET_PRICES_USD.get(fee_curr_code.upper())
                if estimated_fee_curr_price:
                    fee_currency_prices_in_usd[fee_curr_code.upper()] = estimated_fee_curr_price
                    self.logger.debug(f"Used ESTIMATED price for fee currency {fee_curr_code}: ${estimated_fee_curr_price}")
                else:
                     self.logger.warning(f"Could not determine USD price for fee currency '{fee_curr_code}'. Withdrawal fee sorting for this currency might be inaccurate.")
                     fee_currency_prices_in_usd[fee_curr_code.upper()] = None

        def sort_key_for_networks(network_option: Dict[str, Any]) -> Tuple[Decimal, int, int]:
            fee_native = network_option['fee_native']
            fee_curr_upper = network_option.get('fee_currency', asset_code_upper).upper()
            fee_in_usd_equivalent = Decimal('Infinity')

            price_of_fee_asset_in_usd = fee_currency_prices_in_usd.get(fee_curr_upper)

            if price_of_fee_asset_in_usd and price_of_fee_asset_in_usd > 0:
                fee_in_usd_equivalent = fee_native * price_of_fee_asset_in_usd
            elif fee_curr_upper == Config.QUOTE_ASSET.upper():
                 fee_in_usd_equivalent = fee_native
            else:
                self.logger.debug(f"Network '{network_option['normalized_name']}' (fee currency '{fee_curr_upper}') - USD price for fee currency unknown. Sorting with high effective fee.")

            token_pref_score = network_option['priority_score_token']
            general_pref_score = network_option['priority_score_general']
            
            return (fee_in_usd_equivalent, token_pref_score, general_pref_score)

        final_candidate_networks.sort(key=sort_key_for_networks)

        if not final_candidate_networks:
            self.logger.warning(f"No candidate networks remained after sorting for {asset_code_upper} from {from_exchange_id} to {to_exchange_id}.")
            return []

        num_to_log = min(3, len(final_candidate_networks))
        self.logger.info(f"Top {num_to_log} potential networks for {asset_code_upper} ({from_exchange_id} -> {to_exchange_id}) after sorting by fee/preference:")
        for i, net_opt in enumerate(final_candidate_networks[:num_to_log]):
            fee_usd_log_detail = ""
            fee_price = fee_currency_prices_in_usd.get(net_opt['fee_currency'].upper())
            if fee_price and fee_price > 0:
                fee_usd_log_detail = f" (~${(net_opt['fee_native'] * fee_price):.2f} USD)"
            self.logger.info(f"  {i+1}. Normalized: '{net_opt['normalized_name']}', "
                             f"Withdraw Code (on {from_exchange_id}): '{net_opt['withdraw_network_code_on_from_ex']}', "
                             f"Deposit Code (on {to_exchange_id}): '{net_opt['deposit_network_code_on_to_ex']}', "
                             f"Fee: {net_opt['fee_native']} {net_opt.get('fee_currency', asset_code_upper)}{fee_usd_log_detail}, "
                             f"MinW: {net_opt['min_withdrawal_native']} {asset_code_upper}, "
                             f"Scores (Tok/Gen): {net_opt['priority_score_token']}/{net_opt['priority_score_general']}")

        return final_candidate_networks

    async def _check_orderbook_liquidity(self, exchange_id: str, symbol: str, side: str,
                                         amount_base: Decimal, price_target: Decimal,
                                         depth_pct_allowed_slippage: Decimal = Decimal("1.0")
                                         ) -> bool:
        self.logger.debug(f"Liquidity check for {exchange_id} - {symbol} ({side}) "
                          f"Amount: {amount_base:.8f} Base, Price Target: {price_target:.8f}. "
                          f"Slippage allowance: {depth_pct_allowed_slippage}%, Min Market Liquidity: ${Config.MIN_LIQUIDITY_USD}")

        exchange = self.exchanges.get(exchange_id)
        if not exchange or not exchange.has.get('fetchOrderBook'):
            self.logger.warning(f"Exchange {exchange_id} does not support fetchOrderBook or not found. Assuming liquid for {symbol}.")
            return True

        try:
            orderbook = await exchange.fetch_order_book(symbol, limit=20)
            relevant_orders = orderbook['asks' if side == 'buy' else 'bids']

            if not relevant_orders:
                self.logger.warning(f"No {'asks' if side == 'buy' else 'bids'} found in orderbook for {symbol} on {exchange_id}. Assuming not liquid enough.")
                return False

            total_visible_liquidity_usd = sum(Decimal(str(p)) * Decimal(str(a)) for p, a in relevant_orders)
            if total_visible_liquidity_usd < Config.MIN_LIQUIDITY_USD:
                self.logger.warning(
                    f"Liquidity check FAIL (Market Depth): Total visible liquidity for {symbol} on {exchange_id} "
                    f"is only ~${total_visible_liquidity_usd:.2f}, which is below the required minimum of ${Config.MIN_LIQUIDITY_USD}."
                )
                return False
            
            accumulated_base_amount = Decimal('0')
            accumulated_quote_cost = Decimal('0')
            price_limit_buy = price_target * (Decimal('1') + (depth_pct_allowed_slippage / Decimal('100')))
            price_limit_sell = price_target * (Decimal('1') - (depth_pct_allowed_slippage / Decimal('100')))

            for price_level, amount_level in relevant_orders:
                price_dec = Decimal(str(price_level))
                amount_dec = Decimal(str(amount_level))

                if side == 'buy' and price_dec > price_limit_buy:
                    self.logger.debug(f"  Liquidity check (BUY): Price level {price_dec:.8f} exceeds limit {price_limit_buy:.8f}. Stopping accumulation.")
                    break
                if side == 'sell' and price_dec < price_limit_sell:
                    self.logger.debug(f"  Liquidity check (SELL): Price level {price_dec:.8f} is below limit {price_limit_sell:.8f}. Stopping accumulation.")
                    break
                
                can_take_from_level = min(amount_dec, amount_base - accumulated_base_amount)
                accumulated_base_amount += can_take_from_level
                accumulated_quote_cost += can_take_from_level * price_dec
                self.logger.debug(f"  Level: P={price_dec:.8f}, A={amount_dec:.8f}. Took: {can_take_from_level:.8f}. Acc.Base: {accumulated_base_amount:.8f}")

                if accumulated_base_amount >= amount_base:
                    break 

            if accumulated_base_amount < amount_base:
                self.logger.warning(f"Insufficient depth for {symbol} on {exchange_id} to fill {amount_base:.8f} {side.upper()} within slippage. "
                                  f"Could only fill {accumulated_base_amount:.8f} at acceptable prices.")
                return False

            avg_fill_price = accumulated_quote_cost / accumulated_base_amount if accumulated_base_amount > 0 else Decimal(0)

            if side == 'buy':
                if avg_fill_price > price_limit_buy:
                    self.logger.warning(f"Liquidity check FAIL (BUY): Avg fill price {avg_fill_price:.8f} for {symbol} on {exchange_id} "
                                      f"exceeds target {price_target:.8f} with slippage limit {price_limit_buy:.8f}.")
                    return False
            elif side == 'sell':
                if avg_fill_price < price_limit_sell:
                    self.logger.warning(f"Liquidity check FAIL (SELL): Avg fill price {avg_fill_price:.8f} for {symbol} on {exchange_id} "
                                      f"is below target {price_target:.8f} with slippage limit {price_limit_sell:.8f}.")
                    return False

            self.logger.info(f"Liquidity check PASS for {symbol} on {exchange_id} ({side}). "
                             f"Able to fill {accumulated_base_amount:.8f} at avg price {avg_fill_price:.8f} (target: {price_target:.8f}).")
            return True

        except Exception as e:
            self.logger.error(f"Error during liquidity check for {symbol} on {exchange_id}: {e}", exc_info=True)
            return False

# --- Rebalancer Class ---
class Rebalancer:
    def __init__(self, exchanges: Dict[str, ccxt_async.Exchange],
                 balance_manager: 'BalanceManager', analyzer: 'Analyzer'):
        self.exchanges = exchanges
        self.balance_manager = balance_manager
        self.analyzer = analyzer
        self.logger = logging.getLogger(self.__class__.__name__)
        self.active_operations: Dict[str, RebalanceOperation] = {}
        self._config_cache: Dict[str, Dict[str, Any]] = {}

    def _get_exchange_config(self, exchange_id: str) -> Dict[str, Any]:
        if exchange_id not in self._config_cache:
            self._config_cache[exchange_id] = Config.INTERNAL_TRANSFER_CONFIG.get(exchange_id.lower(), {})
        return self._config_cache[exchange_id]

    def _get_account_params(self, exchange_id: str, purpose: str) -> Dict[str, Any]:
        config = self._get_exchange_config(exchange_id)
        return config.get(f'params_for_{purpose}', {})

    def _get_account_type_string(self, exchange_id: str, purpose: str) -> Optional[str]:
        config = self._get_exchange_config(exchange_id)
        account_type = config.get(f'{purpose}_account_type_str')
        if not account_type:
            self.logger.warning(f"No account type string defined for {purpose} on {exchange_id} in INTERNAL_TRANSFER_CONFIG.")
        return account_type

    def _get_withdrawal_wallet_type(self, exchange_id: str) -> Optional[int]:
        config = self._get_exchange_config(exchange_id)
        return config.get('withdrawal_wallet_type')

    def _get_min_transfer_amount(self, exchange_id: str) -> Decimal:
        config = self._get_exchange_config(exchange_id)
        return config.get('min_internal_transfer_amount', Decimal('1e-8'))

    def _create_operation_key(self, asset: str, from_exchange: str, to_exchange: str, amount: Decimal) -> str:
        return f"{asset.upper()}_{from_exchange.lower()}_to_{to_exchange.lower()}_{amount:.8f}"

    def _is_operation_active(self, key: str) -> bool:
        return key in self.active_operations

    def _start_operation(self, key: str, from_exchange: str, to_exchange: str, amount: Decimal, asset: str = "USDT") -> bool:
        if self._is_operation_active(key):
            self.logger.warning(f"Operation {key} already active. Ignoring new request.")
            return False
        self.active_operations[key] = RebalanceOperation(key, from_exchange, to_exchange, amount, asset)
        self.logger.info(f"Started rebalance operation: {key}")
        return True

    def _end_operation(self, key: str) -> None:
        op = self.active_operations.pop(key, None)
        if op: self.logger.info(f"Ended rebalance operation: {key}")
        else: self.logger.debug(f"Attempted to end non-existent operation: {key}")

    async def _check_balance_with_retry(self, exchange: ccxt_async.Exchange, asset: str, description: str, params: Dict[str, Any], max_retries: int = 3, initial_delay_s: float = 1.0) -> Optional[Decimal]:
        for attempt in range(max_retries):
            balance = await self.balance_manager.get_specific_account_balance(exchange, asset, description, params)
            if balance is not None: return balance
            if attempt < max_retries - 1:
                delay = initial_delay_s * (1.5 ** attempt)
                self.logger.debug(f"Retrying balance check for {asset} on {exchange.id} ({description}), attempt {attempt+1}/{max_retries} after {delay:.1f}s delay.")
                await asyncio.sleep(delay)
        self.logger.error(f"Failed to get balance for {asset} on {exchange.id} ({description}) after {max_retries} retries.")
        return None

    @safe_call_wrapper(default_retval=False)
    async def _internal_transfer_if_needed(self, exchange_id: str, asset: str, required_amount_in_target_account: Decimal, from_purpose: str, to_purpose: str, all_balances_snapshot: Dict[str, ExchangeBalance]) -> bool:
        self.logger.debug(f"Rebalancer: _internal_transfer_if_needed called for {asset} on {exchange_id}, from '{from_purpose}' to '{to_purpose}', required: {required_amount_in_target_account:.8f}")
        exchange = self.exchanges.get(exchange_id)
        if not exchange:
            self.logger.error(f"Exchange {exchange_id} not found for internal transfer.")
            return False

        if not (hasattr(exchange, 'has') and exchange.has.get('transfer')):
            self.logger.debug(f"Internal transfers not supported for {exchange_id}. Checking target account balance directly.")
            to_params_direct = self._get_account_params(exchange_id, to_purpose)
            balance_direct = await self._check_balance_with_retry(exchange, asset, f"{to_purpose} account (direct check)", to_params_direct)
            sufficient_direct = balance_direct is not None and balance_direct >= (required_amount_in_target_account - DECIMAL_COMPARISON_EPSILON)
            self.logger.debug(f"Direct balance check for {to_purpose} on {exchange_id}: {balance_direct}, Required: {required_amount_in_target_account:.8f}, Sufficient: {sufficient_direct}")
            return sufficient_direct

        asset_upper = asset.upper()
        from_type = self._get_account_type_string(exchange_id, from_purpose)
        to_type = self._get_account_type_string(exchange_id, to_purpose)
        to_params = self._get_account_params(exchange_id, to_purpose)
        min_transfer = self._get_min_transfer_amount(exchange_id)

        if not from_type or not to_type:
            self.logger.error(f"Missing account types for internal transfer on {exchange_id} ('{from_purpose}' -> '{from_type}', '{to_purpose}' -> '{to_type}'). Cannot proceed.")
            return False

        self.logger.debug(f"Internal transfer check: From type API code '{from_type}', To type API code '{to_type}' on {exchange_id}.")

        if from_type.lower() == to_type.lower():
            self.logger.info(f"Internal transfer from '{from_type}' to '{to_type}' on {exchange_id} is same type. Checking balance in unified account with params: {to_params}.")
            balance_unified = await self._check_balance_with_retry(exchange, asset_upper, f"{to_purpose} (unified) account", to_params)
            if balance_unified is None:
                self.logger.error(f"Failed to fetch balance for unified account check on {exchange_id} for {asset_upper}. Returning False from _internal_transfer_if_needed.")
                return False
            sufficient = balance_unified >= (required_amount_in_target_account - DECIMAL_COMPARISON_EPSILON)
            if not sufficient:
                self.logger.warning(f"Insufficient {asset_upper} ({balance_unified:.8f}) on unified account '{to_type}' for required {required_amount_in_target_account:.8f} on {exchange_id}.")
            else:
                self.logger.info(f"Sufficient {asset_upper} ({balance_unified:.8f}) on unified account '{to_type}' for required {required_amount_in_target_account:.8f} on {exchange_id}. No transfer needed.")
            self.logger.debug(f"_internal_transfer_if_needed: Returning {sufficient} from same-type account check for {exchange_id}/{asset_upper}.")
            return sufficient

        target_bal = await self._check_balance_with_retry(exchange, asset_upper, f"{to_purpose} account", to_params)
        if target_bal is None:
            self.logger.error(f"Could not verify balance in {to_purpose} account for {asset_upper} on {exchange_id}.")
            return False
        if target_bal >= (required_amount_in_target_account - DECIMAL_COMPARISON_EPSILON):
            self.logger.info(f"Sufficient {asset_upper} ({target_bal:.8f}) already in {to_purpose} account on {exchange_id}. No internal transfer needed.")
            return True

        amount_to_transfer = required_amount_in_target_account - target_bal
        self.logger.debug(f"Need to transfer {amount_to_transfer:.8f} {asset_upper} to {to_purpose} account.")
        if amount_to_transfer <= DECIMAL_COMPARISON_EPSILON:
            self.logger.debug(f"Amount to transfer for {asset_upper} to {to_purpose} account is negligible or zero. No transfer initiated.")
            return True

        if amount_to_transfer < min_transfer:
            self.logger.warning(f"Amount to transfer ({amount_to_transfer:.8f} {asset_upper}) is below min ({min_transfer}) for {exchange_id}. Cannot initiate internal transfer. Current target balance: {target_bal:.8f}, required: {required_amount_in_target_account:.8f}")
            return target_bal >= (required_amount_in_target_account - DECIMAL_COMPARISON_EPSILON)

        source_params = self._get_account_params(exchange_id, from_purpose)
        source_bal = await self._check_balance_with_retry(exchange, asset_upper, f"{from_purpose} account as source", source_params)
        if source_bal is None or source_bal < amount_to_transfer:
            self.logger.warning(f"Insufficient {asset_upper} in {from_purpose} account ({source_bal if source_bal is not None else 'Error fetching'}) to transfer {amount_to_transfer:.8f} to {to_purpose} account on {exchange_id}.")
            return False

        self.logger.info(f"Attempting to execute internal transfer of {amount_to_transfer:.8f} {asset_upper} from '{from_type}' to '{to_type}'.")
        return await self._execute_internal_transfer(
            exchange, exchange_id, asset_upper, amount_to_transfer, from_type, to_type, required_amount_in_target_account, to_params
        )

    async def _execute_internal_transfer(self, exchange: ccxt_async.Exchange, exchange_id: str, asset: str, amount: Decimal,
                                       from_type_api: str, to_type_api: str,
                                       required_in_to_final: Decimal, to_params_for_check: Dict[str, Any]) -> bool:
        self.logger.info(f"Attempting internal transfer of {amount:.8f} {asset} from account type '{from_type_api}' to '{to_type_api}' on {exchange_id}.")
        try:
            if Config.DRY_RUN:
                self.logger.info(f"[DRY RUN] Would internally transfer {amount:.8f} {asset} on {exchange_id} from '{from_type_api}' to '{to_type_api}'.")
                res = {'id': f'dry_run_itransfer_{int(time.time())}'}
            else:
                res = await exchange.transfer(asset, float(amount), from_type_api, to_type_api, params={})

            self.logger.info(f"Internal transfer submitted: ID {res.get('id', 'N/A') if res else 'N/A'}. Response: {json.dumps(res, default=str)}")
            await asyncio.sleep(5)

            final_bal_in_target = await self._check_balance_with_retry(exchange, asset, f"target account '{to_type_api}' post-transfer", to_params_for_check)
            success = (final_bal_in_target is not None and final_bal_in_target >= (required_in_to_final - DECIMAL_COMPARISON_EPSILON))

            log_msg = (f"Internal transfer verification {'successful' if success else 'failed'}. "
                       f"Balance in target '{to_type_api}': {final_bal_in_target if final_bal_in_target is not None else 'Error fetching'}, "
                       f"Required: {required_in_to_final:.8f}")
            self.logger.log(logging.INFO if success else logging.WARNING, log_msg)
            return success

        except ccxt_async.BadRequest as e:
            if "cannot make a transfer between" in str(e).lower() and from_type_api.lower() == to_type_api.lower():
                self.logger.warning(f"Internal transfer on {exchange_id} (from '{from_type_api}' to '{to_type_api}') not possible or not needed: {e}. "
                                   "Checking balance directly in the unified/target account.")
                current_bal_unified = await self._check_balance_with_retry(exchange, asset, f"unified account '{to_type_api}' post-BadRequest", to_params_for_check)
                return current_bal_unified is not None and current_bal_unified >= (required_in_to_final - DECIMAL_COMPARISON_EPSILON)
            else:
                self.logger.error(f"BadRequest during internal transfer on {exchange_id} from '{from_type_api}' to '{to_type_api}': {e}", exc_info=True)
                return False
        except ccxt_async.InsufficientFunds:
            self.logger.error(f"Insufficient funds during internal transfer on {exchange_id} from '{from_type_api}' to '{to_type_api}'.")
            return False
        except Exception as e:
            self.logger.error(f"Error during internal transfer on {exchange_id} from '{from_type_api}' to '{to_type_api}': {e}", exc_info=True)
            return False

    async def convert_assets_to_usdt_on_exchange(self, exchange_id: str, balance_snapshot: ExchangeBalance,
                                               min_usd_value: Decimal = Decimal("1.0"),
                                               assets_to_ignore: Optional[Set[str]] = None,
                                               executor_instance: Optional['Executor'] = None ) -> bool:
        self.logger.info(f"Checking for assets to convert to {Config.QUOTE_ASSET} on {exchange_id}...")
        exchange = self.exchanges.get(exchange_id)
        if not exchange or not exchange.markets:
            self.logger.error(f"Cannot convert on {exchange_id}: instance/markets missing."); return False

        if assets_to_ignore is None: assets_to_ignore = set()
        assets_to_ignore.add(Config.QUOTE_ASSET.upper())

        assets_for_conversion = []
        for asset, details in balance_snapshot.assets.items():
            asset_upper = asset.upper()
            if asset_upper not in assets_to_ignore and \
               details.get('free', Decimal(0)) > DECIMAL_COMPARISON_EPSILON and \
               details.get('usd_value', Decimal(0)) >= min_usd_value:
                assets_for_conversion.append(
                    (exchange, exchange_id, asset_upper, details.get('free', Decimal(0)), executor_instance)
                )

        if not assets_for_conversion:
            self.logger.info(f"No assets meet conversion criteria on {exchange_id}.")
            return False

        converted_any = False
        for conv_args in assets_for_conversion:
            try:
                if await self._convert_single_asset(*conv_args):
                    converted_any = True
                    await asyncio.sleep(1)
            except Exception as e:
                 self.logger.error(f"Asset conversion task for {conv_args[2]} on {conv_args[1]} failed: {e}")

        self.logger.info(f"Asset conversion attempts {'finished with at least one success' if converted_any else 'completed with no success'} on {exchange_id}.")
        return converted_any

    async def _convert_single_asset(self, exchange: ccxt_async.Exchange, exchange_id: str, asset: str, amount: Decimal,
                                  executor_instance: Optional['Executor'] = None) -> bool:
        self.logger.info(f"Attempting to convert {amount:.8f} {asset} to {Config.QUOTE_ASSET} on {exchange_id}.")
        symbol = f"{asset}/{Config.QUOTE_ASSET.upper()}"

        market_info = exchange.markets.get(symbol)
        if not market_info:
            self.logger.warning(f"Market {symbol} not found on {exchange_id}. Cannot convert."); return False

        self.logger.debug(f"Market info for {symbol} on {exchange_id}: active={market_info.get('active')}, type={market_info.get('type')}, spot={market_info.get('spot')}")
        self.logger.debug(f"  Limits: amount_min={market_info.get('limits',{}).get('amount',{}).get('min')}, cost_min={market_info.get('limits',{}).get('cost',{}).get('min')}")
        self.logger.debug(f"  Precision: amount={market_info.get('precision',{}).get('amount')}, price={market_info.get('precision',{}).get('price')}")

        min_amount_native_str = market_info.get('limits',{}).get('amount',{}).get('min')
        min_amount_native = Decimal(str(min_amount_native_str)) if min_amount_native_str is not None else Decimal('0')

        min_cost_str = market_info.get('limits',{}).get('cost',{}).get('min')
        min_cost_native = Decimal(str(min_cost_str)) if min_cost_str is not None else Decimal('0')

        if amount < min_amount_native and min_amount_native > 0 :
            self.logger.info(f"Amount {amount:.8f} {asset} is less than minimum tradeable amount {min_amount_native:.8f}. Skipping conversion."); return False

        approx_price_for_cost_check = None
        try:
            if exchange.has.get('fetchTicker'):
                ticker = await exchange.fetch_ticker(symbol)
                if ticker.get('bid') and Decimal(str(ticker['bid'])) > 0:
                    approx_price_for_cost_check = Decimal(str(ticker['bid']))
                elif ticker.get('last') and Decimal(str(ticker['last'])) > 0:
                    approx_price_for_cost_check = Decimal(str(ticker['last']))
        except Exception as e_ticker:
            self.logger.warning(f"Could not fetch ticker for {symbol} on {exchange_id} for cost check: {e_ticker}")

        if approx_price_for_cost_check and min_cost_native > 0:
            estimated_cost_in_quote = amount * approx_price_for_cost_check
            if estimated_cost_in_quote < min_cost_native:
                self.logger.info(f"Estimated cost {estimated_cost_in_quote:.8f} {Config.QUOTE_ASSET} for {amount:.8f} {asset} is less than minimum order cost {min_cost_native:.8f} {Config.QUOTE_ASSET}. Skipping conversion.")
                return False
        elif not approx_price_for_cost_check and min_cost_native > 0:
             self.logger.warning(f"Cannot verify min_cost for {symbol} on {exchange_id} due to missing ticker. Proceeding with caution.")

        amount_adj = amount
        try:
            amount_adj_str = exchange.amount_to_precision(symbol, amount)
            amount_adj = Decimal(amount_adj_str)

            if amount_adj < min_amount_native and min_amount_native > 0:
                self.logger.info(f"Adjusted amount {amount_adj:.8f} {asset} is less than minimum tradeable amount {min_amount_native:.8f}. Skipping conversion."); return False
            if amount_adj <= DECIMAL_COMPARISON_EPSILON:
                self.logger.info(f"Adjusted amount for {asset} is zero or negligible. Skipping conversion."); return False

            if not Config.DRY_RUN and self.analyzer and hasattr(self.analyzer, '_check_orderbook_liquidity'):
                current_bid_price = approx_price_for_cost_check if approx_price_for_cost_check else (await self.analyzer.get_asset_price_in_usdt(asset, exchange_id) or Decimal('0'))
                if current_bid_price > 0:
                    is_liquid_for_conversion = await self.analyzer._check_orderbook_liquidity(
                        exchange_id, symbol, 'sell', amount_adj, current_bid_price
                    )
                    if not is_liquid_for_conversion:
                        self.logger.warning(f"Insufficient order book liquidity on {exchange_id} for selling {amount_adj:.8f} {asset}. Skipping conversion.")
                        return False
                else:
                    self.logger.warning(f"Could not get a valid price for {asset} on {exchange_id} to check liquidity. Skipping liquidity check.")


            order_params_conv = {}
            if exchange.id == 'kucoin' and Config.API_KEYS.get('kucoin',{}).get('password'):
                 order_params_conv = {'tradingPassword': Config.API_KEYS['kucoin']['password']}

            order_resp: Optional[Dict[str, Any]] = None
            initial_order_creation_response: Optional[Dict[str, Any]] = None

            if Config.DRY_RUN:
                self.logger.info(f"[DRY RUN] Would sell {amount_adj} {asset} for {symbol} on {exchange_id}")
                sim_price_for_dry_run = approx_price_for_cost_check or Config.ESTIMATED_ASSET_PRICES_USD.get(asset, Decimal('0.00000001'))
                if sim_price_for_dry_run <= 0: sim_price_for_dry_run = Decimal('0.00000001')

                order_resp = {'id': f'dry_run_convert_{asset}_{int(time.time())}',
                              'symbol': symbol, 'status': 'closed',
                              'filled': float(amount_adj),
                              'cost': float(amount_adj * sim_price_for_dry_run),
                              'average': float(sim_price_for_dry_run),
                              'timestamp': int(time.time()*1000),
                              'fee': {'cost': float(amount_adj * sim_price_for_dry_run * Decimal('0.001')), 'currency': Config.QUOTE_ASSET.upper()}
                             }
            else:
                self.logger.info(f"Attempting exchange.create_market_sell_order for {symbol}, amount: {float(amount_adj)}, params: {order_params_conv}")
                initial_order_creation_response = await exchange.create_market_sell_order(symbol, float(amount_adj), params=order_params_conv)
                self.logger.info(f"INITIAL RESPONSE from create_market_sell_order for {symbol} on {exchange_id}: {json.dumps(initial_order_creation_response, indent=2, default=str)}")

                if not initial_order_creation_response or not initial_order_creation_response.get('id'):
                    self.logger.error(f"No order ID in initial conversion sell response for {symbol}. Response: {initial_order_creation_response}")
                    if initial_order_creation_response and str(initial_order_creation_response.get('status','')).lower() == 'closed' and \
                       Decimal(str(initial_order_creation_response.get('filled','0'))) > DECIMAL_COMPARISON_EPSILON:
                        self.logger.warning(f"Using initial response for {symbol} as final order due to missing ID but closed status.")
                        order_resp = initial_order_creation_response
                    else:
                        raise ccxt_async.ExchangeError("No order ID in initial conversion sell response and order not immediately closed.")

                if not order_resp:
                    order_id = initial_order_creation_response['id']
                    self.logger.info(f"Initial conversion sell order for {asset} on {exchange_id}: ID {order_id}")
                    await asyncio.sleep(3)

                    if executor_instance and hasattr(executor_instance, '_fetch_order_with_retry'):
                        order_resp = await executor_instance._fetch_order_with_retry(exchange, order_id, symbol)
                    else:
                        self.logger.warning("Executor instance or _fetch_order_with_retry not available for _convert_single_asset; using basic fetch_order with retry.")
                        for i_fetch in range(3):
                            try:
                                order_resp = await exchange.fetch_order(order_id, symbol)
                                if order_resp and str(order_resp.get('status')).lower() == 'closed': break
                                await asyncio.sleep(3)
                            except Exception as e_fetch_basic:
                                self.logger.error(f"Fallback fetch_order attempt {i_fetch+1} failed for conversion: {e_fetch_basic}")
                                if i_fetch == 2: order_resp = initial_order_creation_response
                                await asyncio.sleep(3)

            if not order_resp:
                self.logger.error(f"Failed to get final details for conversion sell order of {asset}. Initial response was: {initial_order_creation_response if 'initial_order_creation_response' in locals() else 'N/A'}");
                return False

            order_status_final = str(order_resp.get('status', '')).lower()
            filled_amount_final = Decimal(str(order_resp.get('filled', '0')))

            self.logger.info(f"Final/Fetched conversion sell order details for {asset}: ID {order_resp.get('id', 'N/A')}, Status: {order_status_final}, Filled: {filled_amount_final}, Cost: {order_resp.get('cost', 'N/A')}")

            if order_status_final == 'canceled' and filled_amount_final <= DECIMAL_COMPARISON_EPSILON:
                self.logger.warning(f"Conversion sell order for {asset} (ID: {order_resp.get('id')}) was CANCELED by the exchange, likely due to insufficient liquidity or other rule. Filled: {filled_amount_final}. Raw: {json.dumps(order_resp, indent=2, default=str)}")
                return False

            if order_status_final != 'closed' or filled_amount_final <= DECIMAL_COMPARISON_EPSILON:
                self.logger.error(f"Conversion sell order for {asset} (ID: {order_resp.get('id')}) was not successfully closed or not filled. Status: {order_status_final}, Filled: {filled_amount_final}. Raw: {json.dumps(order_resp, indent=2, default=str)}")
                if order_status_final not in ['closed', 'canceled'] and not Config.DRY_RUN and exchange.has['cancelOrder'] and order_resp.get('id'):
                    try:
                        self.logger.warning(f"Attempting to cancel non-closed conversion order {order_resp.get('id')} for {asset}.")
                        await exchange.cancel_order(order_resp['id'], symbol)
                    except Exception as e_cancel:
                        self.logger.error(f"Failed to cancel conversion order {order_resp.get('id')}: {e_cancel}")
                return False

            if executor_instance and hasattr(executor_instance, '_handle_consolidation_result') and order_resp.get('id'):
                cost_val = order_resp.get('cost', '0')
                filled_val = order_resp.get('filled', '0')
                avg_price_val = order_resp.get('average', '0')

                avg_price_dec = Decimal(str(avg_price_val)) if avg_price_val is not None and Decimal(str(avg_price_val)) > 0 else Decimal('0')
                if avg_price_dec <= 0 and Decimal(str(filled_val)) > 0 and Decimal(str(cost_val)) > 0:
                    avg_price_dec = Decimal(str(cost_val)) / Decimal(str(filled_val))

                details = TradeExecutionDetails(
                    order_id=order_resp.get('id'), timestamp=order_resp.get('timestamp', int(time.time()*1000)),
                    symbol=symbol, side='sell',
                    price=avg_price_dec,
                    amount_base=Decimal(str(filled_val)),
                    cost_quote=Decimal(str(cost_val)),
                    status=order_status_final,
                    raw_response=order_resp
                )
                if isinstance(order_resp.get('fee'), dict):
                    details.fee_amount = Decimal(str(order_resp['fee'].get('cost', 0)))
                    details.fee_currency = order_resp['fee'].get('currency')
                await executor_instance._handle_consolidation_result(exchange_id, asset, details)

            self.logger.info(f"Successfully converted {order_resp.get('filled', 'N/A')} {asset} to approx {order_resp.get('cost', 'N/A')} {Config.QUOTE_ASSET} on {exchange_id}.")
            return True

        except (ccxt_async.InsufficientFunds, ccxt_async.InvalidOrder) as e:
            self.logger.error(f"Order error converting {asset} on {exchange_id}: {type(e).__name__} - {e}. Amount: {amount}, Adjusted: {amount_adj if 'amount_adj' in locals() else 'N/A'}")
            return False
        except ccxt_async.ExchangeError as e:
            self.logger.error(f"CCXT ExchangeError converting {asset} on {exchange_id}: {type(e).__name__} - {e}. Raw initial response if any: {initial_order_creation_response if 'initial_order_creation_response' in locals() else 'N/A'}", exc_info=True)
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error converting {asset} on {exchange_id}: {type(e).__name__} - {e}", exc_info=True)
            return False

    async def _get_asset_transfer_precision(self, exchange_id: str, exchange_instance: ccxt_async.Exchange, asset_code_upper: str) -> Decimal:
        if asset_code_upper == Config.QUOTE_ASSET.upper():
            if exchange_id.lower() == 'binance':
                return Decimal(f'1e-{REBALANCER_BINANCE_USDT_WITHDRAW_PRECISION}')
            return REBALANCER_USDT_TRANSFER_PRECISION

        step: Optional[Decimal] = None
        market_precision_step: Optional[Decimal] = None

        currency_info = self.analyzer.currencies_cache.get(exchange_id, {}).get(asset_code_upper)
        if not currency_info and hasattr(exchange_instance, 'currencies') and exchange_instance.currencies:
             currency_info = exchange_instance.currencies.get(asset_code_upper)

        if currency_info and currency_info.get('precision') is not None:
            try:
                precision_val_curr = currency_info['precision']
                precision_mode_attr = getattr(exchange_instance, 'precisionMode', None)

                TICK_SIZE_MODE_CONST = getattr(ccxt_async.Exchange, 'TICK_SIZE', "TICK_SIZE")
                DECIMAL_PLACES_MODE_CONST = getattr(ccxt_async.Exchange, 'DECIMAL_PLACES', "DECIMAL_PLACES")

                if precision_mode_attr == TICK_SIZE_MODE_CONST:
                     step = Decimal(str(precision_val_curr))
                     self.logger.debug(f"Using currency-level precision (TICK_SIZE from precisionMode) for {asset_code_upper} on {exchange_id}: {step}")
                     return step
                elif precision_mode_attr == DECIMAL_PLACES_MODE_CONST:
                     step = Decimal('1e-' + str(int(Decimal(str(precision_val_curr)))))
                     self.logger.debug(f"Using currency-level precision (DECIMAL_PLACES from precisionMode) for {asset_code_upper} on {exchange_id}: {step} from {precision_val_curr} places")
                     return step
                else:
                    if precision_mode_attr:
                        self.logger.debug(f"Exchange {exchange_id} precisionMode is '{precision_mode_attr}'. Using heuristic for currency precision.")
                    else:
                        self.logger.debug(f"Exchange {exchange_id} does not have a clear precisionMode. Using heuristic for currency precision.")

                    temp_step_curr = Decimal(str(precision_val_curr))
                    if temp_step_curr > 1 and temp_step_curr == round(temp_step_curr):
                         step = Decimal('1e-' + str(int(temp_step_curr)))
                         self.logger.debug(f"Using currency-level precision (heuristic: decimal places) for {asset_code_upper} on {exchange_id}: {step} from {temp_step_curr} places")
                    elif temp_step_curr > 0 and temp_step_curr < 1 :
                         step = temp_step_curr
                         self.logger.debug(f"Using currency-level precision (heuristic: step size) for {asset_code_upper} on {exchange_id}: {step}")
                    else:
                         step = temp_step_curr if isinstance(temp_step_curr, Decimal) else None
                         if step:
                             self.logger.warning(f"Using currency-level precision (direct value) for {asset_code_upper} on {exchange_id}: {step}. Verify this is correct.")
                         else:
                             self.logger.warning(f"Could not determine step from currency precision value {precision_val_curr} for {asset_code_upper} on {exchange_id}.")
                if step is not None: return step
            except Exception as e:
                self.logger.warning(f"Error parsing currency-level precision for {asset_code_upper} on {exchange_id}: {e}")

        if exchange_instance.markets:
            quote_assets_to_try = [Config.QUOTE_ASSET.upper(), "BTC", "ETH"]
            market_to_use_for_precision = None
            for quote in quote_assets_to_try:
                market_candidate = exchange_instance.markets.get(f"{asset_code_upper}/{quote}")
                if market_candidate and market_candidate.get('precision', {}).get('amount') is not None:
                    market_to_use_for_precision = market_candidate
                    break

            if market_to_use_for_precision:
                try:
                    step_val_market = market_to_use_for_precision['precision']['amount']
                    if isinstance(step_val_market, (int, float, str)):
                        market_precision_step = Decimal(str(step_val_market))
                        self.logger.debug(f"Market-derived precision (amount) for {asset_code_upper} on {exchange_id} from pair {market_to_use_for_precision['symbol']}: {market_precision_step}")
                    else:
                        self.logger.warning(f"Market precision 'amount' for {asset_code_upper} on {exchange_id} (pair {market_to_use_for_precision['symbol']}) is not a number/string: {type(step_val_market)}")
                except Exception as e:
                    self.logger.warning(f"Error parsing market precision for {asset_code_upper} on {exchange_id} from pair {market_to_use_for_precision['symbol']}: {e}")

        if market_precision_step is not None:
            self.logger.debug(f"Using market-derived precision as fallback for {asset_code_upper} on {exchange_id}: {market_precision_step}")
            return market_precision_step

        fallback_step = Decimal('1e-8')
        self.logger.warning(f"Using fallback precision step {fallback_step} for {asset_code_upper} on {exchange_id}.")
        return fallback_step

    @safe_call_wrapper(default_retval=False)
    async def transfer_asset_between_exchanges(self, asset_code: str, from_exchange_id: str, to_exchange_id: str,
                                             amount: Decimal, all_balances: Dict[str, ExchangeBalance],
                                             chosen_network_details_override: Optional[Dict[str, Any]] = None) -> bool:
        from_ex_inst = self.exchanges.get(from_exchange_id)
        if not from_ex_inst:
            self.logger.error(f"Source exchange {from_exchange_id} not found for transfer."); return False

        quantize_step = await self._get_asset_transfer_precision(from_exchange_id, from_ex_inst, asset_code.upper())
        quantized_amount = amount.quantize(quantize_step, rounding=ROUND_DOWN)

        if quantized_amount <= DECIMAL_COMPARISON_EPSILON:
            self.logger.warning(f"Amount {amount} for {asset_code} transfer from {from_exchange_id} to {to_exchange_id} "
                                f"became zero/negligible ({quantized_amount}) after quantization with step {quantize_step}. Aborting transfer.")
            return False

        self.logger.info(f"Initiating transfer: {quantized_amount} {asset_code} (original: {amount}, step: {quantize_step}) from {from_exchange_id} to {to_exchange_id}.")
        op_key = self._create_operation_key(asset_code, from_exchange_id, to_exchange_id, quantized_amount)
        if not self._start_operation(op_key, from_exchange_id, to_exchange_id, quantized_amount, asset=asset_code):
            return False

        try:
            return await self._execute_asset_transfer(
                asset_code, from_exchange_id, to_exchange_id,
                quantized_amount, all_balances, chosen_network_details_override
            )
        finally:
            self._end_operation(op_key)

    async def _execute_asset_transfer(self, asset_code: str, from_ex_id: str, to_ex_id: str,
                                    amount_pre_fee: Decimal,
                                    all_balances: Dict[str, ExchangeBalance],
                                    network_details_override: Optional[Dict[str, Any]] = None) -> bool:
        asset_upper = asset_code.upper()

        # --- ИСПРАВЛЕНИЕ ---
        if (from_ex_id.lower(), asset_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
           (to_ex_id.lower(), asset_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST:
            self.logger.error(f"Transfer of {asset_upper} involving {from_ex_id} or {to_ex_id} is blacklisted. Aborting.")
            return False
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

        from_ex = self.exchanges.get(from_ex_id); to_ex = self.exchanges.get(to_ex_id)
        if not from_ex or not to_ex:
            self.logger.error(f"Missing exchange instance for asset transfer ({from_ex_id} or {to_ex_id})."); return False
        if not (hasattr(from_ex, 'has') and from_ex.has.get('withdraw')):
            self.logger.error(f"{from_ex_id} does not support API withdrawals."); return False

        if not await self._prepare_withdrawal_account_generic(from_ex_id, from_ex, asset_upper, amount_pre_fee, all_balances):
             self.logger.error(f"Failed to prepare withdrawal account for {asset_upper} on {from_ex_id}."); return False

        final_network_details_to_use: Optional[Dict[str, Any]] = network_details_override

        if not final_network_details_to_use:
            potential_networks = await self.analyzer._select_optimal_network_for_transfer(asset_upper, from_ex_id, to_ex_id, amount_pre_fee)
            if not potential_networks:
                self.logger.error(f"No potential networks found by Analyzer for {asset_upper} transfer from {from_ex_id} to {to_ex_id}."); return False
            final_network_details_to_use = potential_networks[0]

        if not final_network_details_to_use:
             self.logger.error(f"Critical: No network details available for {asset_upper} transfer from {from_ex_id} to {to_ex_id}."); return False

        self.logger.info(f"Using network for transfer: {final_network_details_to_use.get('normalized_name')} "
                         f"(Withdraw API Code on {from_ex_id}: {final_network_details_to_use.get('withdraw_network_code_on_from_ex')}, "
                         f"Deposit API Code on {to_ex_id}: {final_network_details_to_use.get('deposit_network_code_on_to_ex')})")

        depo_addr = await self._get_deposit_address_generic(to_ex, asset_upper, final_network_details_to_use)
        if not depo_addr:
            self.logger.error(f"Failed to get deposit address for {asset_upper} on {to_ex_id} via network {final_network_details_to_use.get('normalized_name')}."); return False

        return await self._execute_withdrawal_generic(from_ex, from_ex_id, asset_upper, amount_pre_fee, final_network_details_to_use, depo_addr)

    async def _prepare_withdrawal_account_generic(self, ex_id: str, ex_inst: ccxt_async.Exchange, asset: str, amount: Decimal, all_balances: Dict[str, ExchangeBalance]) -> bool:
        self.logger.debug(f"Preparing withdrawal account on {ex_id} for {amount:.8f} {asset}.")
        wd_params = self._get_account_params(ex_id, "withdrawal")
        wd_type_str = self._get_account_type_string(ex_id, "withdrawal")

        bal = await self._check_balance_with_retry(ex_inst, asset, f"withdrawal account ('{wd_type_str}')", wd_params)
        if bal is None:
            self.logger.error(f"Failed to get {asset} balance in withdrawal account of {ex_id}."); return False

        if bal >= (amount - DECIMAL_COMPARISON_EPSILON):
            self.logger.info(f"Sufficient {asset} ({bal:.8f}) in withdrawal account on {ex_id}."); return True

        self.logger.info(f"{asset} in withdrawal account on {ex_id} ({bal:.8f}) < desired {amount:.8f}. Attempting internal transfer.")
        if not await self._internal_transfer_if_needed(ex_id, asset, amount, "trading", "withdrawal", all_balances):
            self.logger.error(f"Failed to ensure {asset} in withdrawal account on {ex_id} via internal transfer."); return False

        bal_after = await self._check_balance_with_retry(ex_inst, asset, f"withdrawal account (post-transfer)", wd_params)
        if bal_after is None or bal_after < (amount - DECIMAL_COMPARISON_EPSILON):
            self.logger.error(f"Still insufficient {asset} in withdrawal account on {ex_id} ({bal_after if bal_after is not None else 'Error fetching'}) after transfer attempt. Required: {amount:.8f}."); return False

        self.logger.info(f"Successfully ensured {bal_after:.8f} {asset} in withdrawal account on {ex_id}."); return True

    def _are_networks_compatible(self, requested_normalized: str, returned_normalized: str) -> bool:
        if requested_normalized == returned_normalized:
            return True
        if returned_normalized == "DEFAULT" and requested_normalized != "UNKNOWN_NETWORK":
            self.logger.info(f"Network compatibility: Exchange returned 'DEFAULT' for requested '{requested_normalized}'. Assuming compatible.")
            return True
        if (requested_normalized not in ["DEFAULT", "UNKNOWN_NETWORK"] and returned_normalized in ["DEFAULT", "UNKNOWN_NETWORK"]):
            self.logger.warning(f"Network compatibility check: Requested specific '{requested_normalized}', but got generic '{returned_normalized}'. Considered incompatible for safety if not DEFAULT.")
            return False
        if requested_normalized != returned_normalized:
            self.logger.warning(f"Network compatibility check: Requested '{requested_normalized}', but got '{returned_normalized}'. Considered incompatible.")
            return False
        return True

    # --- ИСПРАВЛЕННЫЙ МЕТОД ---
    async def _get_deposit_address_generic(self, to_exchange: ccxt_async.Exchange, asset_code: str, network_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # --- ИСПРАВЛЕНИЕ: Заменено ASSET_TRANSFER_BLACKLIST на ASSET_UNAVAILABLE_BLACKLIST ---
        if (to_exchange.id.lower(), asset_code.upper()) in Config.ASSET_UNAVAILABLE_BLACKLIST:
            self.logger.error(f"Deposit of {asset_code.upper()} to {to_exchange.id} is blacklisted. Cannot get/create deposit address.")
            return None
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

        api_network_code_for_deposit = network_info.get('deposit_network_code_on_to_ex')
        normalized_network_name_requested = network_info['normalized_name']

        if not api_network_code_for_deposit or \
           api_network_code_for_deposit in ['UNKNOWN_ORIGINAL_CODE_ON_TO_EX', 'UNKNOWN_NETWORK', 'DEFAULT']:
            if normalized_network_name_requested not in ['UNKNOWN_NETWORK', 'DEFAULT']:
                self.logger.debug(f"Deposit network code for {to_exchange.id} API was generic or missing in network_info. "
                                  f"Using requested normalized name: '{normalized_network_name_requested}' as API param.")
                api_network_code_for_deposit = normalized_network_name_requested
            else:
                self.logger.debug(f"Deposit network code and requested normalized name are generic for {to_exchange.id}. "
                                  f"Will attempt fetch without explicit network first (api_network_code_for_deposit=None).")
                api_network_code_for_deposit = None 

        self.logger.info(f"Fetching/creating deposit address for {asset_code} on {to_exchange.id} "
                         f"using initial API network param: '{api_network_code_for_deposit if api_network_code_for_deposit else 'None (try default first)'}'. "
                         f"Requested normalized context: '{normalized_network_name_requested}'")

        deposit_address_details: Optional[Dict[str, Any]] = None
        delay_after_creation_s = 25
        fetch_params: Dict[str, Any] = {}

        if api_network_code_for_deposit:
            fetch_params = {'network': api_network_code_for_deposit}
            try:
                self.logger.debug(f"Attempt 1: Fetching deposit address for {asset_code} on {to_exchange.id} with specific network param: '{api_network_code_for_deposit}'")
                if to_exchange.id.lower() == 'gateio' and asset_code.upper() == 'GAT' and api_network_code_for_deposit != 'NAC':
                    self.logger.warning(f"Overriding network for GAT on Gate.io to NAC. Original API code was: {api_network_code_for_deposit}")
                    fetch_params['network'] = 'NAC' 

                deposit_address_details = await to_exchange.fetch_deposit_address(asset_code, params=fetch_params)
                if deposit_address_details and deposit_address_details.get('address'):
                    self.logger.info(f"Successfully fetched deposit address (with specific network '{fetch_params['network']}').")
            except (ccxt_async.InvalidAddress, ccxt_async.BadRequest, ccxt_async.NotSupported) as e_fetch_specific:
                if "deposit disabled" in str(e_fetch_specific).lower():
                    self.logger.error(f"Deposit for {asset_code} (API: {fetch_params.get('network')}) on {to_exchange.id} is DISABLED: {e_fetch_specific}")
                    return None
                elif "network was not found" in str(e_fetch_specific).lower() or "can not determine the default network" in str(e_fetch_specific).lower() or "Unknown network" in str(e_fetch_specific): 
                    self.logger.warning(f"Fetch with specific network '{fetch_params.get('network')}' failed (network not found/default issue for {asset_code} on {to_exchange.id}): {e_fetch_specific}. Will try other methods.")
                else:
                    self.logger.warning(f"Fetch with specific network '{fetch_params.get('network')}' failed for {asset_code} on {to_exchange.id}: {e_fetch_specific}. Will try other methods.")
                deposit_address_details = None
            except Exception as e_fetch_other:
                self.logger.error(f"Error fetching deposit address with specific network '{fetch_params.get('network')}' for {asset_code} on {to_exchange.id}: {e_fetch_other}", exc_info=True)
                deposit_address_details = None

        if not (deposit_address_details and deposit_address_details.get('address')):
            try:
                self.logger.debug(f"Attempt 2: Fetching deposit address for {asset_code} on {to_exchange.id} without explicit network param (to find default).")
                temp_details_no_net = await to_exchange.fetch_deposit_address(asset_code, params={})
                if temp_details_no_net and temp_details_no_net.get('address'):
                    returned_raw_net_no_param = temp_details_no_net.get('network')
                    returned_norm_net_no_param = Config.normalize_network_name_for_config(returned_raw_net_no_param)
                    self.logger.info(f"Successfully fetched default deposit address. Returned network: '{returned_raw_net_no_param}' (Normalized: '{returned_norm_net_no_param}')")
                    if self._are_networks_compatible(normalized_network_name_requested, returned_norm_net_no_param):
                        deposit_address_details = temp_details_no_net
                        api_network_code_for_deposit = returned_raw_net_no_param
                    else:
                        self.logger.warning(f"Default deposit address network '{returned_norm_net_no_param}' is NOT compatible with requested '{normalized_network_name_requested}'.")
                else:
                    self.logger.warning(f"Attempt 2 (no network param): Still no address from fetch_deposit_address for {asset_code}. Response: {temp_details_no_net}")
            except (ccxt_async.ExchangeError, ccxt_async.NotSupported) as e_fetch_no_net_param:
                 if "deposit disabled" in str(e_fetch_no_net_param).lower():
                    self.logger.error(f"Deposit for {asset_code} on {to_exchange.id} is DISABLED (default network attempt): {e_fetch_no_net_param}")
                    return None
                 elif "can not determine the default network" in str(e_fetch_no_net_param).lower() or "network not found" in str(e_fetch_no_net_param).lower():
                    self.logger.warning(f"Fetch without network param failed (default network issue for {asset_code} on {to_exchange.id}): {e_fetch_no_net_param}.")
                 else:
                    self.logger.error(f"Error on attempt 2 fetching deposit address (no network param) for {asset_code} on {to_exchange.id}: {e_fetch_no_net_param}", exc_info=False) 
            except Exception as e_fetch_no_net_param_unexp:
                 self.logger.error(f"Unexpected error on attempt 2 fetching deposit address (no network param) for {asset_code} on {to_exchange.id}: {e_fetch_no_net_param_unexp}", exc_info=True)

        if not (deposit_address_details and deposit_address_details.get('address')) and \
           hasattr(to_exchange, 'has') and to_exchange.has.get('createDepositAddress'):
            create_params = {'network': api_network_code_for_deposit} if api_network_code_for_deposit else {}
            self.logger.info(f"Attempt 3: Creating deposit address for {asset_code} on {to_exchange.id} (API param for create: '{api_network_code_for_deposit}').")

            try:
                created_info = await to_exchange.create_deposit_address(asset_code, params=create_params)
                self.logger.info(f"Create deposit address response: {json.dumps(created_info, default=str)}")
                if created_info and created_info.get('address'):
                    deposit_address_details = created_info
                else:
                    self.logger.info(f"Address not directly in create response. Waiting {delay_after_creation_s}s and re-fetching with params for create: {create_params}.")
                    await asyncio.sleep(delay_after_creation_s)
                    deposit_address_details = await to_exchange.fetch_deposit_address(asset_code, params=create_params) 
            except ccxt_async.ExchangeError as e_create:
                if to_exchange.id.lower() == 'mexc' and ("152071" in str(e_create) or "deposit address reached the limit" in str(e_create).lower() or "152073" in str(e_create)):
                    self.logger.critical(f"MEXC Error (Address Limit/Multi-Address Not Supported): {e_create} for {asset_code} (Network: {api_network_code_for_deposit}). Manual intervention on MEXC may be required or network not suitable for new address.")
                    return None
                elif "deposit disabled" in str(e_create).lower() or "deposit forbidden" in str(e_create).lower():
                     self.logger.error(f"Deposit for {asset_code} (API: {api_network_code_for_deposit}) on {to_exchange.id} is disabled/forbidden during creation: {e_create}")
                     return None
                else:
                    self.logger.error(f"ExchangeError creating deposit address for {asset_code} (Network: {api_network_code_for_deposit}): {type(e_create).__name__} - {e_create}")
            except Exception as e_unexp_create:
                self.logger.error(f"Unexpected error creating deposit address for {asset_code} (Network: {api_network_code_for_deposit}): {e_unexp_create}", exc_info=True)

        if not deposit_address_details or not deposit_address_details.get('address'):
            self.logger.error(f"Failed to obtain a valid deposit address for {asset_code} on {to_exchange.id} for network '{normalized_network_name_requested}' (API code attempted: '{api_network_code_for_deposit}') after all attempts.")
            return None

        self.logger.info(f"Final Obtained deposit address on {to_exchange.id} for {asset_code}: {deposit_address_details['address']} "
                         f"(Tag: {deposit_address_details.get('tag')}) Net from exchange: {deposit_address_details.get('network', 'N/A')}")

        returned_network_raw = deposit_address_details.get('network')
        if returned_network_raw:
            returned_network_normalized = Config.normalize_network_name_for_config(returned_network_raw)
            if not self._are_networks_compatible(normalized_network_name_requested, returned_network_normalized):
                self.logger.error(f"CRITICAL: Final deposit address for {asset_code} on {to_exchange.id} is for network '{returned_network_normalized}' (raw: '{returned_network_raw}'), "
                                  f"but requested was '{normalized_network_name_requested}'. Incompatible.")
                return None
        elif api_network_code_for_deposit:
            self.logger.warning(f"Exchange {to_exchange.id} did not specify a network in the deposit address response for {asset_code}, "
                                f"though network '{api_network_code_for_deposit}' was requested/used for API. Proceeding with caution, assuming it's for the requested network.")
            deposit_address_details['network'] = api_network_code_for_deposit

        if to_exchange.id.lower() == 'mexc':
            assets_requiring_memo_on_mexc = {"XRP", "XLM", "ATOM", "CRO", "HBAR", "EOS", "BTS", "TON"}
            asset_upper_for_check = asset_code.upper()
            if asset_upper_for_check in assets_requiring_memo_on_mexc:
                retrieved_tag = deposit_address_details.get('tag')
                if not retrieved_tag:
                    self.logger.critical(f"CRITICAL VALIDATION FAILURE (MEXC): Asset {asset_code} on MEXC requires a deposit tag/memo for network "
                                         f"{normalized_network_name_requested} (API code used: {api_network_code_for_deposit}), but NO TAG was returned. Transfer aborted.")
                    return None
                else:
                    self.logger.info(f"MEXC Validation: Asset {asset_code} requires a tag, and a tag '{retrieved_tag}' was found.")

        return deposit_address_details

    async def _execute_withdrawal_generic(self, from_exchange: ccxt_async.Exchange, from_ex_id: str, asset: str,
                                        amount: Decimal, net_info: Dict[str, Any], depo_addr: Dict[str, Any]) -> bool:
        asset_upper = asset.upper()

        # --- ИСПРАВЛЕНИЕ ---
        if (from_ex_id.lower(), asset_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
           (depo_addr.get('exchange_id','').lower(), asset_upper) in Config.ASSET_UNAVAILABLE_BLACKLIST:
            self.logger.error(f"Withdrawal/Transfer of {asset_upper} involving {from_ex_id} or {depo_addr.get('exchange_id','TARGET_EXCHANGE')} is blacklisted. Aborting operation.")
            return False
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

        try:
            wd_net_code = net_info['withdraw_network_code_on_from_ex']
            net_fee_native = net_info['fee_native']
            fee_curr = net_info.get('fee_currency', asset_upper)
            min_wd_native = net_info.get('min_withdrawal_native', Decimal('0'))

            amount_for_api_call = amount

            if amount_for_api_call < min_wd_native:
                self.logger.error(f"API Withdrawal amount {amount_for_api_call:.8f} {asset_upper} is less than minimum withdrawal {min_wd_native:.8f} for network '{wd_net_code}'."); return False

            if fee_curr.upper() == asset_upper and amount_for_api_call <= net_fee_native:
                self.logger.error(f"API Withdrawal amount {amount_for_api_call:.8f} {asset_upper} is not greater than network fee {net_fee_native:.8f} {fee_curr}. Net amount to send would be <= 0."); return False

            elif fee_curr.upper() != Config.QUOTE_ASSET.upper() and fee_curr.upper() != asset_upper:
                fee_bal = await self.balance_manager._check_balance_with_retry(from_exchange, fee_curr, f"fee currency ({fee_curr}) check for withdrawal", self._get_account_params(from_ex_id, "withdrawal"))
                if fee_bal is None or fee_bal < net_fee_native:
                    self.logger.error(f"Insufficient balance of fee currency {fee_curr} ({fee_bal if fee_bal is not None else 'Error fetching'}) for network fee {net_fee_native:.8f}. Required on withdrawal account."); return False

            wd_params = {'network': wd_net_code}
            addr_tag_str = depo_addr.get('tag')

            if from_ex_id.lower() == 'binance':
                bin_wd_acc_type_str = self._get_account_type_string(from_ex_id, "withdrawal")
                if bin_wd_acc_type_str and bin_wd_acc_type_str.upper() == 'FUNDING':
                    wallet_type_from_config = self._get_withdrawal_wallet_type(from_ex_id)
                    if wallet_type_from_config is not None:
                        wd_params['walletType'] = wallet_type_from_config

            target_addr_str = depo_addr['address']

            amount_after_network_fee_est = amount - net_fee_native if fee_curr.upper() == asset_upper else amount

            self.logger.info(f"Attempting withdrawal of {amount_for_api_call:.8f} {asset_upper} (intended net PRE-NETWORK-FEE: {amount:.8f}) "
                             f"from {from_ex_id} to {target_addr_str} (Tag: {addr_tag_str if addr_tag_str else 'None'}) via network '{wd_net_code}'. "
                             f"Network Fee: {net_fee_native:.8f} {fee_curr}. Est. arrival (after network fee): {amount_after_network_fee_est:.8f} {asset_upper}.")

            wd_info = None
            if Config.DRY_RUN:
                self.logger.info(f"[DRY RUN] Withdrawal: code={asset_upper}, amount={float(amount_for_api_call)}, address={target_addr_str}, tag={addr_tag_str}, params={wd_params}")
                wd_info = {'id': f'dry_run_wd_{asset_upper}_{int(time.time())}'}
            else:
                wd_info = await from_exchange.withdraw(
                    code=asset_upper,
                    amount=float(amount_for_api_call),
                    address=target_addr_str,
                    tag=addr_tag_str,
                    params=wd_params
                )

            self.logger.info(f"Withdrawal request for {asset_upper} submitted. Response: {json.dumps(wd_info, indent=2, default=str) if wd_info else 'No response or error in submission'}")
            return True

        except ccxt_async.InsufficientFunds as e:
            self.logger.error(f"Insufficient funds for {asset_upper} withdrawal from {from_ex_id} (amount_for_api_call: {amount_for_api_call}, or for fee {fee_curr}): {e}", exc_info=True)
            return False
        except ccxt_async.ExchangeError as e:
            self.logger.error(f"ExchangeError during {asset_upper} withdrawal from {from_ex_id}: {type(e).__name__} - {e}", exc_info=True)
            return False
        except Exception as e:
            self.logger.error(f"Error executing {asset_upper} withdrawal from {from_ex_id}: {type(e).__name__} - {e}", exc_info=True)
            return False

    @safe_call_wrapper(default_retval=False)
    async def ensure_usdt_for_trade(self, exchange_id_to_fund: str, amount_usdt_needed_on_target: Decimal,
                                  all_balances: Dict[str, ExchangeBalance],
                                  preferred_source_exchange_id: Optional[str] = 'binance',
                                  executor_instance: Optional['Executor'] = None) -> bool:
        self.logger.info(f"Ensuring {amount_usdt_needed_on_target:.8f} USDT on {exchange_id_to_fund} for trade.")
        quote_asset_upper = Config.QUOTE_ASSET.upper()
        amount_to_request_pre_fee = (amount_usdt_needed_on_target + Config.TRANSFER_FEE_BUFFER_USD).quantize(REBALANCER_USDT_TRANSFER_PRECISION, rounding=ROUND_DOWN)
        self.logger.info(f"JIT funding: deficit {amount_usdt_needed_on_target:.8f}, requesting ~{amount_to_request_pre_fee:.8f} (incl. buffer).")

        potential_usdt_sources = []
        if preferred_source_exchange_id and preferred_source_exchange_id != exchange_id_to_fund:
            bal_data = all_balances.get(preferred_source_exchange_id)
            if bal_data and bal_data.assets.get(quote_asset_upper, {}).get('free', Decimal(0)) - Config.MIN_REMAINING_USD_TO_LEAVE_ON_EX >= amount_to_request_pre_fee:
                potential_usdt_sources.append(preferred_source_exchange_id)

        for ex_id, bal_data in all_balances.items():
            if ex_id == exchange_id_to_fund or ex_id == preferred_source_exchange_id: continue
            if bal_data.assets.get(quote_asset_upper, {}).get('free', Decimal(0)) - Config.MIN_REMAINING_USD_TO_LEAVE_ON_EX >= amount_to_request_pre_fee:
                if ex_id not in potential_usdt_sources: potential_usdt_sources.append(ex_id)

        for source_ex_id in potential_usdt_sources:
            self.logger.info(f"Attempting JIT USDT transfer from {source_ex_id} to {exchange_id_to_fund}.")

            usdt_transfer_networks = await self.analyzer._select_optimal_network_for_transfer(
                quote_asset_upper, source_ex_id, exchange_id_to_fund, amount_to_request_pre_fee
            )
            if not usdt_transfer_networks:
                self.logger.warning(f"No suitable networks found by Analyzer for JIT USDT transfer from {source_ex_id} to {exchange_id_to_fund}.")
                continue

            transfer_successful_for_source = False
            for net_details in usdt_transfer_networks:
                self.logger.info(f"Attempting JIT USDT transfer: {amount_to_request_pre_fee:.8f} from {source_ex_id} to {exchange_id_to_fund} via network {net_details.get('normalized_name')}.")
                if await self.transfer_asset_between_exchanges(quote_asset_upper, source_ex_id, exchange_id_to_fund, amount_to_request_pre_fee, all_balances, chosen_network_details_override=net_details):
                    self.logger.info(f"Initiated JIT USDT transfer from {source_ex_id} via {net_details.get('normalized_name')}. Waiting for arrival will be handled by the caller (Executor).");
                    transfer_successful_for_source = True
                    break
                else:
                    self.logger.warning(f"Failed JIT USDT transfer attempt from {source_ex_id} to {exchange_id_to_fund} via network {net_details.get('normalized_name')}.")

            if transfer_successful_for_source:
                return True

        self.logger.info("No direct USDT source found or all transfer attempts failed. Attempting conversion of other liquid assets on other exchanges.")
        for source_ex_id, balance_data in all_balances.items():
            if source_ex_id == exchange_id_to_fund: continue

            source_ex_inst = self.exchanges.get(source_ex_id)
            if not source_ex_inst: continue

            for asset_code, asset_details in balance_data.assets.items():
                asset_upper = asset_code.upper()
                if asset_upper == quote_asset_upper or asset_upper not in Config.JIT_LIQUID_ASSETS_FOR_CONVERSION:
                    continue

                free_native = asset_details.get('free', Decimal(0))
                if free_native <= DECIMAL_COMPARISON_EPSILON: continue

                price_usdt = await self.analyzer.get_asset_price_in_usdt(asset_upper, preferred_ref_exchange_id=source_ex_id)
                if not price_usdt or price_usdt <= 0:
                    self.logger.debug(f"No valid USDT price for {asset_upper} on {source_ex_id} for JIT conversion estimate."); continue

                free_usd_val = free_native * price_usdt
                if free_usd_val < Config.JIT_FUNDING_MIN_CONVERSION_USD:
                    self.logger.debug(f"Asset {asset_upper} on {source_ex_id} value {free_usd_val:.2f} USD < min JIT conversion {Config.JIT_FUNDING_MIN_CONVERSION_USD} USD. Skipping."); continue

                effective_price_after_sell_fee = price_usdt * (Decimal('1') - Decimal('0.002'))
                if effective_price_after_sell_fee <= 0: continue

                native_needed_for_target_usdt = amount_to_request_pre_fee / effective_price_after_sell_fee

                buffer_to_leave_native = (Config.MIN_REMAINING_USD_TO_LEAVE_ON_EX / price_usdt) if price_usdt > 0 else Decimal('Infinity')
                sellable_native_max = free_native - buffer_to_leave_native

                amount_to_convert_native = min(sellable_native_max, native_needed_for_target_usdt)

                if amount_to_convert_native <= DECIMAL_COMPARISON_EPSILON: continue

                est_usdt_from_this_conversion = amount_to_convert_native * effective_price_after_sell_fee

                if est_usdt_from_this_conversion >= amount_usdt_needed_on_target:
                    self.logger.info(f"Found {amount_to_convert_native:.8f} {asset_upper} (worth ~{est_usdt_from_this_conversion:.2f} USDT after est. fee) on {source_ex_id}. Attempting conversion.")
                    if await self._convert_single_asset(source_ex_inst, source_ex_id, asset_upper, amount_to_convert_native, executor_instance):
                        await asyncio.sleep(5)

                        current_balances_post_conv = await self.balance_manager.get_all_balances(calculate_usd_values=False)
                        usdt_on_source_after_conv = current_balances_post_conv.get(source_ex_id, ExchangeBalance(source_ex_id)).assets.get(quote_asset_upper,{}).get('free', Decimal(0))

                        available_usdt_for_transfer = usdt_on_source_after_conv - Config.MIN_REMAINING_USD_TO_LEAVE_ON_EX
                        amount_to_actually_transfer = min(available_usdt_for_transfer, amount_to_request_pre_fee)

                        if amount_to_actually_transfer >= amount_usdt_needed_on_target:
                            usdt_transfer_networks_post_conv = await self.analyzer._select_optimal_network_for_transfer(
                                quote_asset_upper, source_ex_id, exchange_id_to_fund, amount_to_actually_transfer
                            )
                            if not usdt_transfer_networks_post_conv:
                                self.logger.warning(f"No suitable networks found for JIT USDT transfer from {source_ex_id} to {exchange_id_to_fund} after conversion.")
                                continue

                            transfer_successful_post_conv = False
                            for net_details_post_conv in usdt_transfer_networks_post_conv:
                                self.logger.info(f"Attempting USDT transfer after converting {asset_upper}: {amount_to_actually_transfer:.8f} USDT from {source_ex_id} to {exchange_id_to_fund} via {net_details_post_conv.get('normalized_name')}.")
                                if await self.transfer_asset_between_exchanges(quote_asset_upper, source_ex_id, exchange_id_to_fund, amount_to_actually_transfer, current_balances_post_conv, chosen_network_details_override=net_details_post_conv):
                                    self.logger.info(f"Initiated USDT transfer from {source_ex_id} (post-conversion via {net_details_post_conv.get('normalized_name')}). Waiting for arrival handled by caller.");
                                    transfer_successful_post_conv = True
                                    break
                            if transfer_successful_post_conv:
                                return True
                        else:
                            self.logger.warning(f"Insufficient USDT on {source_ex_id} after converting {asset_upper} to meet original deficit. Available for transfer: {available_usdt_for_transfer:.8f}, Original deficit: {amount_usdt_needed_on_target:.8f}")
                    else:
                        self.logger.warning(f"Failed to convert {asset_upper} to USDT on {source_ex_id}.")

        self.logger.error(f"Exhausted all JIT funding options for {exchange_id_to_fund}."); return False

    def get_active_operations(self) -> List[Dict[str, Any]]:
        return [{'key': op.key, 'from_exchange': op.from_exchange, 'to_exchange': op.to_exchange, 'amount': float(op.amount), 'asset': op.asset, 'duration_seconds': time.time() - op.created_at} for op in self.active_operations.values()]

    def cleanup_stale_operations(self, max_age_seconds: int = 7200) -> int:
        stale_keys = [key for key, op in self.active_operations.items() if time.time() - op.created_at > max_age_seconds]
        for key in stale_keys:
            op_details = self.active_operations[key]
            self.logger.warning(f"Removing stale operation: Key='{op_details.key}', Asset='{op_details.asset}', Amount='{op_details.amount}'")
            self._end_operation(key)
        return len(stale_keys)
    
# --- Executor Class ---
class Executor:
    def __init__(self,
                 exchanges: Dict[str, ccxt_async.Exchange],
                 balance_manager: BalanceManager,
                 analyzer: 'Analyzer', # For liquidity checks, network info
                 rebalancer: 'Rebalancer'): # For JIT funding, internal transfers
        self.exchanges = exchanges
        self.balance_manager = balance_manager
        self.analyzer = analyzer
        self.rebalancer = rebalancer
        self.logger = logging.getLogger(self.__class__.__name__)
        self.active_trades: Set[str] = set() # Stores unique_ids of ongoing trades
        self.trade_history: List[CompletedArbitrageLog] = []

    def _log_trade_event(self, log_entry: CompletedArbitrageLog, event_message: str, is_error:bool = False, exc_info_dump=False):
        level = logging.ERROR if is_error else logging.INFO
        self.logger.log(level, f"TRADE_LOG [{log_entry.opportunity_id}] Status: {log_entry.status} - {event_message}", exc_info=exc_info_dump)
        if is_error and log_entry: # Append to existing error message if any
            current_error = log_entry.error_message + "; " if log_entry.error_message else ""
            log_entry.error_message = f"{current_error}{event_message}"

    async def _handle_consolidation_result(self, exchange_id: str, asset_code: str, consolidation_details: TradeExecutionDetails):
        # This is a callback from Rebalancer._convert_single_asset
        # For now, just log. Could be expanded to update internal state or trigger other actions.
        self.logger.info(f"Executor received consolidation result: {asset_code} on {exchange_id}, Order ID: {consolidation_details.order_id}, Status: {consolidation_details.status}, Cost: {consolidation_details.cost_quote}")
        if consolidation_details.status and 'close' in consolidation_details.status.lower() and consolidation_details.cost_quote:
            # Potentially add this to a separate consolidation history or update balances
            pass


    async def _wait_for_asset_arrival(self, exchange_id: str, exchange_instance: ccxt_async.Exchange, 
                                    asset: str, expected_min_amount_increase: Decimal, 
                                    account_params: Dict[str, Any], # Params for specific account (e.g., withdrawal)
                                    log_entry: CompletedArbitrageLog, # For logging trade progress
                                    wait_seconds: int, check_interval: int = 60) -> Optional[Decimal]: # Returns final balance on account if arrival confirmed
        asset_upper = asset.upper()
        self.logger.info(f"Waiting for at least {expected_min_amount_increase:.8f} {asset_upper} to arrive (net increase) on {exchange_id} (account: {account_params}). Max wait: {wait_seconds}s, Check interval: {check_interval}s.")
        num_checks = (wait_seconds // check_interval) + 1 if check_interval > 0 else 1

        # Get initial balance in the target account before waiting
        initial_balance_before_wait = await self.balance_manager.get_specific_account_balance(exchange_instance, asset_upper, f"initial check before arrival wait on {exchange_id}", account_params)
        if initial_balance_before_wait is None:
            self.logger.error(f"Failed to get initial balance for {asset_upper} on {exchange_id} before waiting for arrival. Aborting wait.")
            self._log_trade_event(log_entry, f"Failed to get initial balance for {asset_upper} on {exchange_id} before waiting.", is_error=True)
            return None
        self.logger.info(f"Initial balance of {asset_upper} on {exchange_id} (account for arrival) before waiting: {initial_balance_before_wait:.8f}")
        
        last_checked_balance: Optional[Decimal] = initial_balance_before_wait # Keep track of last good balance

        for i in range(num_checks):
            if i > 0: # Don't sleep on the first check
                self.logger.debug(f"Asset arrival check {i+1}/{num_checks} for {asset_upper}. Sleeping {check_interval}s.")
                await asyncio.sleep(check_interval)

            current_balance = await self.balance_manager.get_specific_account_balance(exchange_instance, asset_upper, f"arrival check on {exchange_id}", account_params)

            if current_balance is not None:
                last_checked_balance = current_balance # Update last known good balance
                arrived_amount_net = current_balance - initial_balance_before_wait # Calculate net increase
                self.logger.info(f"Arrival check {i+1}: {asset_upper} on {exchange_id}. Current total: {current_balance:.8f}, Initial: {initial_balance_before_wait:.8f}, Net arrived since wait started: {arrived_amount_net:.8f}. Expected min net arrival: {expected_min_amount_increase:.8f}")

                if arrived_amount_net >= (expected_min_amount_increase - DECIMAL_COMPARISON_EPSILON): # Check if net increase meets expectation
                    self._log_trade_event(log_entry, f"Asset {asset_upper} (net arrived: {arrived_amount_net:.8f}, total on account: {current_balance:.8f}) confirmed on {exchange_id}.")
                    return current_balance # Asset arrived
            else: # Failed to fetch balance this check
                self.logger.warning(f"Arrival check {i+1}: Failed to fetch balance for {asset_upper} on {exchange_id}.")
                # Continue trying, using last_checked_balance for any intermediate decisions if needed

        # If loop finishes without confirming arrival
        self._log_trade_event(log_entry, f"Asset {asset_upper} did not arrive in sufficient quantity (net increase) on {exchange_id} after {wait_seconds}s. Expected min net arrival: {expected_min_amount_increase:.8f}. Last total balance check: {last_checked_balance if last_checked_balance is not None else 'N/A'}. Aborting.", is_error=True)
        return None # Asset did not arrive in time


    async def _fetch_order_with_retry(self, exchange: ccxt_async.Exchange, order_id: str, symbol: str, max_retries: int = 7, delay_s: int = 6) -> Optional[Dict[str, Any]]:
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Attempt {attempt+1}/{max_retries} to fetch order {order_id} ({symbol}) on {exchange.id}")
                order_details = await exchange.fetch_order(order_id, symbol)

                if not order_details: # fetch_order returned None (should be rare for valid ID)
                    self.logger.warning(f"fetch_order for {order_id} returned None on {exchange.id}, attempt {attempt+1}.")
                    if attempt < max_retries - 1: await asyncio.sleep(delay_s); continue
                    else: # All retries exhausted for None response
                        self.logger.error(f"Order {order_id} ({symbol}) on {exchange.id} still not found or None after {max_retries} attempts.")
                        return None # Could not fetch

                # Parse key details (filled, remaining, status)
                filled_amount_str = order_details.get('filled')
                filled_amount = Decimal(str(filled_amount_str)) if filled_amount_str is not None else Decimal(0)
                
                remaining_amount_str = order_details.get('remaining')
                # Calculate remaining if not directly provided but amount and filled are
                if remaining_amount_str is None and order_details.get('amount') is not None and filled_amount_str is not None:
                    try:
                        total_amount = Decimal(str(order_details['amount']))
                        remaining_amount = total_amount - filled_amount
                    except (InvalidOperation, TypeError): # Handle parsing errors
                        remaining_amount = Decimal(0) # Default if calculation fails
                else: # Use 'remaining' if available
                    remaining_amount = Decimal(str(remaining_amount_str)) if remaining_amount_str is not None else Decimal(0)
                
                order_status = str(order_details.get('status', '')).lower() # Normalize status

                if order_status == 'closed':
                    self.logger.info(f"Order {order_id} fetched and is CLOSED. Filled: {filled_amount}")
                    return order_details
                elif order_status == 'canceled':
                    self.logger.warning(f"Order {order_id} ({symbol}) on {exchange.id} is CANCELED. Filled: {filled_amount}. Raw: {json.dumps(order_details, indent=2, default=str)}")
                    return order_details # Return canceled order details
                elif order_status == 'open' or 'partially' in order_status: # Still open or partially filled
                    self.logger.info(f"Order {order_id} ({symbol}) on {exchange.id} is still '{order_status}'. Filled: {filled_amount}, Remaining: {remaining_amount}.")
                    if attempt == max_retries - 1: # Last attempt and still open
                        self.logger.error(f"Order {order_id} ({symbol}) on {exchange.id} is still '{order_status}' after {max_retries} attempts. Considering it problematic for immediate arbitrage.")
                        return order_details # Return its current (problematic) state
                else: # Other statuses (e.g., 'expired', 'rejected')
                     self.logger.info(f"Order {order_id} fetched with status '{order_status}'. Filled: {filled_amount}, Remaining: {remaining_amount}. Raw: {json.dumps(order_details, indent=2, default=str)}")
                     return order_details # Return details for other terminal statuses

            except ccxt_async.OrderNotFound:
                self.logger.warning(f"Order {order_id} not found on {exchange.id} on attempt {attempt+1}.")
                # For OrderNotFound, often better to fail faster if it's a persistent issue.
                # If after 2 attempts it's still not found, it's likely a permanent issue or very slow propagation.
                if attempt >= 1 : # Allow one retry for very quick propagation delays
                    self.logger.error(f"Order {order_id} ({symbol}) on {exchange.id} definitively not found after {attempt+1} attempts.")
                    return None # Order truly not found
            except (ccxt_async.NetworkError, ccxt_async.RequestTimeout) as e_net: # Network issues
                self.logger.warning(f"Network error fetching order {order_id}, attempt {attempt+1}: {e_net}")
            except Exception as e: # Other unexpected errors
                self.logger.error(f"Unexpected error fetching order {order_id}, attempt {attempt+1}: {e}", exc_info=True)

            if attempt < max_retries - 1: # If not the last attempt, sleep and retry
                self.logger.debug(f"Waiting {delay_s}s before retrying fetch_order for {order_id}.")
                await asyncio.sleep(delay_s)

        self.logger.error(f"Failed to fetch conclusive details for order {order_id} on {exchange.id} after {max_retries} retries."); 
        return None # All retries failed to get a conclusive state

    async def _check_liquidity_for_trade_leg(self, exchange_id: str, symbol: str, side: str, # 'buy' or 'sell'
                                           amount_to_trade: Decimal, # Can be in quote (for buy) or base (for sell)
                                           price_target: Decimal, # Expected price for the trade
                                           is_base_amount: bool = False # True if amount_to_trade is in base asset
                                           ) -> bool:
        if not self.analyzer or not hasattr(self.analyzer, '_check_orderbook_liquidity'):
            self.logger.warning("Analyzer or _check_orderbook_liquidity not available in Executor. Assuming liquid for safety (or implement stricter default).")
            return True # Default to true if analyzer/method not found (can be changed for stricter behavior)

        amount_base_equivalent: Decimal
        if is_base_amount: # If amount_to_trade is already in base asset
            amount_base_equivalent = amount_to_trade
        else: # If amount_to_trade is in quote asset (e.g., USD for a buy order)
            if price_target <= 0: # Cannot convert if price is invalid
                self.logger.warning(f"Cannot calculate base equivalent for liquidity check: price_target is {price_target} for {symbol} on {exchange_id}")
                return False # Cannot assess liquidity
            amount_base_equivalent = amount_to_trade / price_target
        
        if amount_base_equivalent <= DECIMAL_COMPARISON_EPSILON:
            self.logger.info(f"Calculated base amount for liquidity check is negligible ({amount_base_equivalent}). Assuming liquid enough for {symbol} on {exchange_id}.")
            return True

        return await self.analyzer._check_orderbook_liquidity(exchange_id, symbol, side, amount_base_equivalent, price_target)

    # --- ИСПРАВЛЕННЫЙ МЕТОД ---
    @safe_call_wrapper(default_retval=False) 
    async def execute_arbitrage(self, opp: ArbitrageOpportunity, all_balances_snapshot: Dict[str, ExchangeBalance]) -> bool:
        trade_key = opp.get_unique_id()

        # --- ИСПРАВЛЕНИЕ: Заменено ASSET_TRANSFER_BLACKLIST на ASSET_UNAVAILABLE_BLACKLIST ---
        if (opp.buy_exchange_id.lower(), opp.base_asset.upper()) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
           (opp.sell_exchange_id.lower(), opp.base_asset.upper()) in Config.ASSET_UNAVAILABLE_BLACKLIST:
            self.logger.warning(f"Skipping arbitrage opportunity {trade_key} involving generally blacklisted asset/exchange: {opp.base_asset} on {opp.buy_exchange_id} or {opp.sell_exchange_id}")
            return False
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

        if trade_key in self.active_trades: 
            self.logger.warning(f"Trade {trade_key} already active (or recently completed and not yet cleared)."); return False
        self.active_trades.add(trade_key)

        trade_log_entry = CompletedArbitrageLog(
            opportunity_id=trade_key,
            timestamp_start=int(time.time() * 1000),
            buy_exchange=opp.buy_exchange_id,
            sell_exchange=opp.sell_exchange_id,
            symbol=opp.symbol,
            original_opportunity_details={ 
                'buy_price': str(opp.buy_price), 'sell_price': str(opp.sell_price),
                'gross_profit_pct': str(opp.gross_profit_pct),
                'net_profit_pct_estimate': str(opp.net_profit_pct) if opp.net_profit_pct is not None else "N/A",
                'chosen_network': opp.chosen_network,
                'withdrawal_fee_usd_estimate': str(opp.withdrawal_fee_usd) if opp.withdrawal_fee_usd is not None else "N/A",
                'is_liquid_enough_for_trade': opp.is_liquid_enough_for_trade
            }
        )
        self.trade_history.append(trade_log_entry) 

        self.logger.info(f"--- Executing FULL Arbitrage Opportunity ---")
        self._log_trade_event(trade_log_entry, f"Starting: {opp.get_unique_id()} | Est. Net: {opp.net_profit_pct if opp.net_profit_pct is not None else Decimal('NaN'):.3f}% | Liquid (pre-check): {opp.is_liquid_enough_for_trade}")

        if not opp.is_liquid_enough_for_trade:
            self._log_trade_event(trade_log_entry, f"Opportunity {trade_key} did not pass pre-execution liquidity check by Analyzer. Aborting.", is_error=True)
            trade_log_entry.status = "SETUP_ERROR_INSUFFICIENT_LIQUIDITY_PRE_CHECK"
            self.active_trades.discard(trade_key); return False

        buy_ex = self.exchanges.get(opp.buy_exchange_id)
        sell_ex = self.exchanges.get(opp.sell_exchange_id)
        if not buy_ex or not sell_ex:
            self._log_trade_event(trade_log_entry, f"Exchange instance not found for {opp.buy_exchange_id} or {opp.sell_exchange_id}.", is_error=True)
            trade_log_entry.status = "SETUP_ERROR_EXCHANGE_INSTANCE"
            self.active_trades.discard(trade_key); return False

        quote_asset, base_asset = Config.QUOTE_ASSET.upper(), opp.base_asset.upper()
        target_buy_cost_usdt = max(Config.TRADE_AMOUNT_USD, Config.MIN_EFFECTIVE_TRADE_USD).quantize(USDT_TRANSFER_PRECISION_CONFIG) 

        if opp.buy_price <= 0: 
            self._log_trade_event(trade_log_entry, f"Invalid buy price ({opp.buy_price}). Cannot calculate base amount.", is_error=True)
            trade_log_entry.status = "INVALID_DATA_PRICE_ERROR"
            self.active_trades.discard(trade_key); return False
        amount_base_to_buy_approx = target_buy_cost_usdt / opp.buy_price 
        self.logger.debug(f"Approx base to buy: {amount_base_to_buy_approx:.8f} {base_asset} for cost {target_buy_cost_usdt:.8f} {quote_asset}")

        # --- BUY LEG ---
        trade_log_entry.status = "BUY_LEG_PENDING"
        self._log_trade_event(trade_log_entry, f"Stage 1: Preparing to buy {base_asset} on {opp.buy_exchange_id}")

        buy_ex_trading_params = self.rebalancer._get_account_params(opp.buy_exchange_id, "trading")
        usdt_on_buy_ex = await self.balance_manager.get_specific_account_balance(buy_ex, quote_asset, "trading account initial check", buy_ex_trading_params)

        if usdt_on_buy_ex is None: 
            self._log_trade_event(trade_log_entry, f"Could not fetch USDT balance on {opp.buy_exchange_id} for buy leg.", is_error=True)
            trade_log_entry.status = "BUY_LEG_FAILED_BALANCE_CHECK"
            self.active_trades.discard(trade_key); return False

        if usdt_on_buy_ex < (target_buy_cost_usdt - DECIMAL_COMPARISON_EPSILON):
            self.logger.info(f"Insufficient USDT on {opp.buy_exchange_id} trading account ({usdt_on_buy_ex:.8f}). Need {target_buy_cost_usdt:.8f}. Attempting local asset conversion first.")
            buy_ex_balance_data = all_balances_snapshot.get(opp.buy_exchange_id)
            if buy_ex_balance_data and buy_ex_balance_data.assets:
                assets_to_ignore_for_local_conv = {base_asset, quote_asset} 
                converted_locally = await self.rebalancer.convert_assets_to_usdt_on_exchange(
                    exchange_id=opp.buy_exchange_id, balance_snapshot=buy_ex_balance_data,
                    min_usd_value=Config.JIT_FUNDING_MIN_CONVERSION_USD, assets_to_ignore=assets_to_ignore_for_local_conv,
                    executor_instance=self 
                )
                if converted_locally: 
                    self.logger.info(f"Performed local asset conversion on {opp.buy_exchange_id}. Re-checking USDT balance.")
                    await asyncio.sleep(5) 
                    usdt_on_buy_ex = await self.balance_manager.get_specific_account_balance(buy_ex, quote_asset, "trading account after local conversion", buy_ex_trading_params)
                    if usdt_on_buy_ex is None: 
                        self._log_trade_event(trade_log_entry, f"Could not fetch USDT balance on {opp.buy_exchange_id} after local conversion.", is_error=True);
                        trade_log_entry.status = "BUY_LEG_FAILED_BALANCE_CHECK_POST_CONV"
                        self.active_trades.discard(trade_key); return False
                    self._log_trade_event(trade_log_entry, f"USDT balance on {opp.buy_exchange_id} trading account after local conversion: {usdt_on_buy_ex:.8f}")
            else:
                self.logger.info(f"No balance data or no other assets found on {opp.buy_exchange_id} for local conversion.")

        if usdt_on_buy_ex < (target_buy_cost_usdt - DECIMAL_COMPARISON_EPSILON):
            deficit = target_buy_cost_usdt - usdt_on_buy_ex
            self._log_trade_event(trade_log_entry, f"Still insufficient USDT on {opp.buy_exchange_id} trading account ({usdt_on_buy_ex:.8f}). Need {target_buy_cost_usdt:.8f} (Deficit: {deficit:.8f}). Attempting JIT funding from other exchanges.")

            latest_balances_for_jit = await self.balance_manager.get_all_balances(calculate_usd_values=True)
            if not latest_balances_for_jit:
                self._log_trade_event(trade_log_entry, "Failed to fetch latest balances for JIT funding from other exchanges.", is_error=True)
                trade_log_entry.status = "JIT_FUNDING_FAILED_BALANCE_CHECK"
                self.active_trades.discard(trade_key); return False

            funded_from_other_ex = await self.rebalancer.ensure_usdt_for_trade(
                opp.buy_exchange_id, deficit.quantize(USDT_TRANSFER_PRECISION_CONFIG), 
                latest_balances_for_jit, executor_instance=self
            )
            if not funded_from_other_ex: 
                self._log_trade_event(trade_log_entry, f"JIT funding process failed to initiate transfer for {opp.buy_exchange_id}.", is_error=True)
                trade_log_entry.status = "JIT_FUNDING_FAILED_TRANSFER_INIT"
                self.active_trades.discard(trade_key); return False

            usdt_on_buy_ex_after_jit = await self._wait_for_asset_arrival(
                opp.buy_exchange_id, buy_ex, quote_asset, 
                deficit.quantize(USDT_TRANSFER_PRECISION_CONFIG), 
                buy_ex_trading_params, trade_log_entry, Config.JIT_FUNDING_WAIT_S
            )
            if usdt_on_buy_ex_after_jit is None or usdt_on_buy_ex_after_jit < (target_buy_cost_usdt - DECIMAL_COMPARISON_EPSILON):
                self._log_trade_event(trade_log_entry, f"Still insufficient USDT ({usdt_on_buy_ex_after_jit if usdt_on_buy_ex_after_jit is not None else 'Error fetching'}) after JIT funding and wait. Required total: {target_buy_cost_usdt:.8f}", is_error=True)
                trade_log_entry.status = "JIT_FUNDING_FAILED_INSUFFICIENT_POST_WAIT"
                self.active_trades.discard(trade_key); return False
            usdt_on_buy_ex = usdt_on_buy_ex_after_jit 
            self._log_trade_event(trade_log_entry, f"JIT Funding from other exchanges successful. Total USDT on {opp.buy_exchange_id} trading account: {usdt_on_buy_ex:.8f}")
        else:
            self._log_trade_event(trade_log_entry, f"Sufficient USDT ({usdt_on_buy_ex:.8f}) on {opp.buy_exchange_id} trading account (possibly after local conversion).")

        if not await self.rebalancer._internal_transfer_if_needed(opp.buy_exchange_id, quote_asset, target_buy_cost_usdt, "withdrawal", "trading", all_balances_snapshot): 
            self._log_trade_event(trade_log_entry, f"Failed to ensure USDT in 'trading' account on {opp.buy_exchange_id} via internal transfer before buy.", is_error=True)
            trade_log_entry.status = "BUY_LEG_FAILED_INTERNAL_TRANSFER_USDT"
            self.active_trades.discard(trade_key); return False

        actual_amount_bought_base_net_fee: Optional[Decimal] = None
        buy_order_id: Optional[str] = None; buy_order_cost_usdt: Optional[Decimal] = None
        buy_order_avg_price: Optional[Decimal] = None; buy_fee_details_parsed: Optional[Dict[str, Any]] = None
        buy_order_status_parsed: str = "UNKNOWN"; raw_buy_order_response: Optional[Dict[str, Any]] = None

        try:
            self.logger.info(f"Placing market BUY order for {opp.symbol} on {opp.buy_exchange_id} (target cost: ~{target_buy_cost_usdt:.8f} {quote_asset}, approx base: ~{amount_base_to_buy_approx:.8f} {base_asset}).")
            market_details_buy = buy_ex.markets.get(opp.symbol, {})
            if not market_details_buy: raise ValueError(f"Market details for {opp.symbol} not found on {opp.buy_exchange_id}")

            use_cost_based_order = buy_ex.has.get('createMarketBuyOrderWithCost', False)
            if opp.buy_exchange_id == 'gateio' and not Config.API_KEYS.get('gateio',{}).get('options', {}).get('createMarketBuyOrderRequiresPrice', True):
                use_cost_based_order = True 
            if opp.buy_exchange_id == 'mexc': 
                use_cost_based_order = False 

            order_params_buy = {} 
            if buy_ex.id == 'kucoin' and Config.API_KEYS.get('kucoin',{}).get('password'):
                order_params_buy = {'tradingPassword': Config.API_KEYS['kucoin']['password']}
            
            initial_order_response: Optional[Dict[str, Any]] = None
            if Config.DRY_RUN:
                self.logger.info(f"[DRY RUN] Would place market BUY for {opp.symbol} on {opp.buy_exchange_id} (cost ~{target_buy_cost_usdt:.8f} or amount ~{amount_base_to_buy_approx:.8f})")
                raw_buy_order_response = { 
                    'id': f'dryrun_buy_{int(time.time())}', 'symbol': opp.symbol, 'status': 'closed',
                    'filled': float(amount_base_to_buy_approx), 'cost': float(target_buy_cost_usdt),
                    'average': float(opp.buy_price), 'timestamp': int(time.time() * 1000),
                    'fee': {'cost': float(amount_base_to_buy_approx * (opp.buy_fee_pct or Decimal('0.1')) / Decimal('100')), 'currency': base_asset} if opp.buy_fee_pct else None
                }
            elif use_cost_based_order:
                min_cost_str = market_details_buy.get('limits',{}).get('cost',{}).get('min')
                min_cost = Decimal(str(min_cost_str)) if min_cost_str is not None else Decimal('0')
                if target_buy_cost_usdt < min_cost and min_cost > 0: 
                    self.logger.warning(f"Target buy cost {target_buy_cost_usdt:.8f} < min market cost {min_cost:.8f}. Switching to amount-based for {opp.buy_exchange_id}.")
                    use_cost_based_order = False
                else:
                    self.logger.info(f"Using createMarketBuyOrderWithCost for {opp.symbol} on {opp.buy_exchange_id}: cost={float(target_buy_cost_usdt)}")
                    initial_order_response = await buy_ex.create_market_buy_order_with_cost(opp.symbol, float(target_buy_cost_usdt), params=order_params_buy)
            
            if not Config.DRY_RUN and not use_cost_based_order:
                min_amount_str_buy = market_details_buy.get('limits', {}).get('amount', {}).get('min')
                min_amount_buy = Decimal(str(min_amount_str_buy)) if min_amount_str_buy is not None else Decimal('0')

                amount_to_buy_precise_str = buy_ex.amount_to_precision(opp.symbol, amount_base_to_buy_approx)
                amount_to_buy_for_order_call = Decimal(amount_to_buy_precise_str)

                if amount_to_buy_for_order_call < min_amount_buy and min_amount_buy > 0:
                    cost_for_min_amount = min_amount_buy * opp.buy_price 
                    usdt_bal_for_min_check = await self.balance_manager.get_specific_account_balance(buy_ex, quote_asset, "min_amount buy check", buy_ex_trading_params) or Decimal(0)
                    if cost_for_min_amount > target_buy_cost_usdt and cost_for_min_amount > usdt_bal_for_min_check :
                        raise ccxt_async.InsufficientFunds(f"Cannot afford min order amount {min_amount_buy} {base_asset} (costs ~{cost_for_min_amount:.2f} {quote_asset}), have {usdt_bal_for_min_check:.8f} {quote_asset}")
                    self.logger.warning(f"Calculated buy amount {amount_to_buy_for_order_call} {base_asset} for {opp.symbol} is less than exchange minimum {min_amount_buy}. Attempting to buy minimum amount.")
                    amount_to_buy_for_order_call = min_amount_buy 
                
                self.logger.info(f"Executing createMarketBuyOrder for {opp.symbol} on {opp.buy_exchange_id}: amount={float(amount_to_buy_for_order_call)}")
                initial_order_response = await buy_ex.create_market_buy_order(opp.symbol, float(amount_to_buy_for_order_call), params=order_params_buy)

            if not Config.DRY_RUN:
                self.logger.info(f"Initial buy order response: {json.dumps(initial_order_response, indent=2, default=str) if initial_order_response else 'No response'}")
                if not initial_order_response or not initial_order_response.get('id'): 
                    if initial_order_response and str(initial_order_response.get('status','')).lower() == 'closed' and \
                       Decimal(str(initial_order_response.get('filled','0'))) > DECIMAL_COMPARISON_EPSILON: 
                        self.logger.warning(f"Using initial response for buy order {opp.symbol} as final due to missing ID but closed status.")
                        raw_buy_order_response = initial_order_response
                    else:
                        raise ccxt_async.ExchangeError("No order ID in buy response and order not immediately closed.")
                
                if not raw_buy_order_response: 
                    buy_order_id = initial_order_response['id']
                    await asyncio.sleep(3) 
                    raw_buy_order_response = await self._fetch_order_with_retry(buy_ex, buy_order_id, opp.symbol)
                
                if not raw_buy_order_response: 
                    raise ccxt_async.ExchangeError(f"Failed to fetch final details for buy order {buy_order_id if buy_order_id else 'ID_UNKNOWN'}.")

            status_fetch = raw_buy_order_response.get('status'); buy_order_status_parsed = str(status_fetch).upper() if status_fetch else "UNKNOWN"

            if buy_order_status_parsed != "CLOSED":
                self.logger.error(f"Buy order {buy_order_id or raw_buy_order_response.get('id')} did not close. Status: {buy_order_status_parsed}. Raw: {json.dumps(raw_buy_order_response, indent=2, default=str)}")
                if buy_order_status_parsed not in ["CANCELED", "REJECTED", "EXPIRED"] and buy_ex.has['cancelOrder'] and (buy_order_id or raw_buy_order_response.get('id')) and not Config.DRY_RUN:
                    try: 
                        order_id_to_cancel = buy_order_id or raw_buy_order_response.get('id')
                        self.logger.warning(f"Attempting to cancel non-closed buy order {order_id_to_cancel} for {opp.symbol}.")
                        await buy_ex.cancel_order(order_id_to_cancel, opp.symbol)
                    except Exception as e_cancel_buy:
                        self.logger.error(f"Failed to cancel non-closed buy order {order_id_to_cancel}: {e_cancel_buy}")
                raise OrderNotClosedError(f"Buy order {buy_order_id or raw_buy_order_response.get('id')} status: {buy_order_status_parsed}")

            if opp.buy_exchange_id.lower() == 'gateio' and 'info' in raw_buy_order_response and isinstance(raw_buy_order_response['info'], dict):
                info_data = raw_buy_order_response['info']
                self.logger.debug(f"Gate.io 'info' field for buy order {raw_buy_order_response.get('id')}: {info_data}")
                
                filled_str_info = info_data.get('filled_amount') or info_data.get('amount') 
                actual_amount_bought_base_gross = Decimal(str(filled_str_info)) if filled_str_info is not None else Decimal('0')
                
                cost_str_info = info_data.get('filled_total') 
                buy_order_cost_usdt = Decimal(str(cost_str_info)) if cost_str_info is not None else Decimal('0')
                
                avg_price_str_info = info_data.get('avg_deal_price')
                if avg_price_str_info is not None and Decimal(str(avg_price_str_info)) > 0:
                    buy_order_avg_price = Decimal(str(avg_price_str_info))
                elif actual_amount_bought_base_gross > 0 and buy_order_cost_usdt > 0: 
                    buy_order_avg_price = buy_order_cost_usdt / actual_amount_bought_base_gross
                else: buy_order_avg_price = opp.buy_price

                fee_cost_str_info = info_data.get('fee'); fee_currency_str_info = info_data.get('fee_currency')
                if fee_cost_str_info is not None and fee_currency_str_info is not None:
                    buy_fee_details_parsed = {'currency': fee_currency_str_info.upper(), 'cost': Decimal(str(fee_cost_str_info))}
                else: 
                    fees_list_std = raw_buy_order_response.get('fees', [])
                    if not fees_list_std and raw_buy_order_response.get('fee'): fees_list_std = [raw_buy_order_response.get('fee')] 
                    if fees_list_std and isinstance(fees_list_std[0], dict):
                        fee_info_std = fees_list_std[0]
                        if fee_info_std.get('cost') is not None and fee_info_std.get('currency') is not None:
                             buy_fee_details_parsed = {'currency': fee_info_std['currency'].upper(), 'cost': Decimal(str(fee_info_std['cost']))}
                self.logger.debug(f"Gate.io parsed: Gross Amount={actual_amount_bought_base_gross}, Cost={buy_order_cost_usdt}, AvgPrice={buy_order_avg_price}, Fee={buy_fee_details_parsed}")
            else: 
                filled_str = raw_buy_order_response.get('filled')
                actual_amount_bought_base_gross = Decimal(str(filled_str)) if filled_str is not None and Decimal(str(filled_str)) > DECIMAL_COMPARISON_EPSILON else None
                
                if actual_amount_bought_base_gross is None and buy_order_status_parsed == 'CLOSED':
                    amount_str = raw_buy_order_response.get('amount')
                    if amount_str is not None and Decimal(str(amount_str)) > DECIMAL_COMPARISON_EPSILON:
                        actual_amount_bought_base_gross = Decimal(str(amount_str))
                        self.logger.info(f"Used 'amount' as filled for closed buy order {buy_order_id or raw_buy_order_response.get('id')}.")

                cost_str = raw_buy_order_response.get('cost'); avg_price_str = raw_buy_order_response.get('average')
                buy_order_cost_usdt = Decimal(str(cost_str)) if cost_str is not None and Decimal(str(cost_str)) > 0 else \
                                      (actual_amount_bought_base_gross * Decimal(str(avg_price_str)) if avg_price_str and Decimal(str(avg_price_str)) > 0 and actual_amount_bought_base_gross else \
                                      (actual_amount_bought_base_gross * opp.buy_price if actual_amount_bought_base_gross else Decimal(0)))
                buy_order_avg_price = Decimal(str(avg_price_str)) if avg_price_str and Decimal(str(avg_price_str)) > 0 else \
                                      (buy_order_cost_usdt / actual_amount_bought_base_gross if actual_amount_bought_base_gross and buy_order_cost_usdt > 0 and actual_amount_bought_base_gross > 0 else opp.buy_price)
                
                fees_list = raw_buy_order_response.get('fees', [])
                if not fees_list and raw_buy_order_response.get('fee'): fees_list = [raw_buy_order_response.get('fee')]
                if fees_list and isinstance(fees_list[0], dict):
                    fee_info = fees_list[0]
                    if fee_info.get('cost') is not None and fee_info.get('currency') is not None:
                        buy_fee_details_parsed = {'currency': fee_info['currency'].upper(), 'cost': Decimal(str(fee_info['cost']))}

            if actual_amount_bought_base_gross is None: 
                raise OrderNotClosedError(f"Buy order {buy_order_id or raw_buy_order_response.get('id')} not filled or filled amount is zero after parsing. Status: {buy_order_status_parsed}.")

            actual_amount_bought_base_net_fee = actual_amount_bought_base_gross
            if buy_fee_details_parsed and buy_fee_details_parsed['currency'] == base_asset.upper():
                actual_amount_bought_base_net_fee = (actual_amount_bought_base_gross or Decimal(0)) - buy_fee_details_parsed['cost']
            
            if not actual_amount_bought_base_net_fee or actual_amount_bought_base_net_fee <= DECIMAL_COMPARISON_EPSILON:
                raise ValueError(f"Buy order resulted in zero or negligible net {base_asset} after fees. Gross: {actual_amount_bought_base_gross}, Fee: {buy_fee_details_parsed}")

            self.logger.info(f"Buy Leg Final: Net Amount {actual_amount_bought_base_net_fee:.8f} {base_asset}, Cost {buy_order_cost_usdt:.2f} USDT, Avg Price {buy_order_avg_price:.8f}")
            trade_log_entry.buy_leg = TradeExecutionDetails(
                order_id=buy_order_id or raw_buy_order_response.get('id'), timestamp=raw_buy_order_response.get('timestamp', int(time.time()*1000)),
                symbol=opp.symbol, side='buy', price=buy_order_avg_price, 
                amount_base=actual_amount_bought_base_net_fee, cost_quote=buy_order_cost_usdt,
                fee_amount=buy_fee_details_parsed.get('cost') if buy_fee_details_parsed else None, 
                fee_currency=buy_fee_details_parsed.get('currency') if buy_fee_details_parsed else None, 
                status=buy_order_status_parsed, raw_response=raw_buy_order_response
            )
            trade_log_entry.initial_buy_cost_usdt = buy_order_cost_usdt
            trade_log_entry.net_base_asset_after_buy_fee = actual_amount_bought_base_net_fee
            trade_log_entry.status = "BUY_LEG_FILLED"

        except (ccxt_async.InsufficientFunds, ccxt_async.InvalidOrder, ccxt_async.ExchangeError, ValueError, OrderNotClosedError) as e_buy:
            self._log_trade_event(trade_log_entry, f"Error in buy leg: {type(e_buy).__name__} - {e_buy}",is_error=True,exc_info_dump=True)
            trade_log_entry.status="BUY_LEG_FAILED"
            self.active_trades.discard(trade_key); return False
        except Exception as e_unexp: 
            self._log_trade_event(trade_log_entry, f"Unexpected buy error: {type(e_unexp).__name__} - {e_unexp}",is_error=True,exc_info_dump=True)
            trade_log_entry.status="BUY_LEG_FAILED_UNEXPECTED"
            self.active_trades.discard(trade_key); return False
        
        await asyncio.sleep(3) 

        # --- TRANSFER LEG ---
        if actual_amount_bought_base_net_fee is None: 
            self._log_trade_event(trade_log_entry, "Critical: net_base_after_buy_fee is None before transfer leg.", is_error=True)
            trade_log_entry.status = "TRANSFER_LEG_FAILED_PRE_CHECK"
            self.active_trades.discard(trade_key); return False

        trade_log_entry.status = "TRANSFER_LEG_PENDING"
        self._log_trade_event(trade_log_entry, f"Stage 2: Transferring {actual_amount_bought_base_net_fee:.8f} {base_asset} from {opp.buy_exchange_id} to {opp.sell_exchange_id}")
        
        if not opp.potential_networks:
            self._log_trade_event(trade_log_entry, f"No potential networks found in opportunity object for withdrawal of {base_asset} from {opp.buy_exchange_id}.", is_error=True)
            trade_log_entry.status = "TRANSFER_LEG_FAILED_NO_NETWORK_LIST"
            self.active_trades.discard(trade_key); return False

        transfer_initiated_successfully = False
        used_network_details: Optional[Dict[str, Any]] = None
        
        if not opp.chosen_network_details: 
            self._log_trade_event(trade_log_entry, f"Opportunity {opp.get_unique_id()} does not have chosen_network_details set. Cannot proceed with transfer.", is_error=True)
            trade_log_entry.status = "TRANSFER_LEG_FAILED_NO_CHOSEN_NETWORK"
            self.active_trades.discard(trade_key); return False

        current_chosen_network_details = opp.chosen_network_details
        current_chosen_network_name = opp.chosen_network 
        
        self.logger.info(f"Attempting transfer using the pre-selected network from Analyzer: {current_chosen_network_name} (Withdraw API: {current_chosen_network_details.get('withdraw_network_code_on_from_ex')})")

        if not await self.rebalancer._internal_transfer_if_needed(opp.buy_exchange_id, base_asset, actual_amount_bought_base_net_fee, "trading", "withdrawal", all_balances_snapshot):
            self._log_trade_event(trade_log_entry, f"Failed internal transfer of {base_asset} to withdrawal account on {opp.buy_exchange_id} (for network {current_chosen_network_name}). This is a critical error for this leg.", is_error=True)
            trade_log_entry.status = "TRANSFER_LEG_FAILED_INTERNAL_TRANSFER_BASE"
            self.active_trades.discard(trade_key); return False

        depo_addr = await self.rebalancer._get_deposit_address_generic(sell_ex, base_asset, current_chosen_network_details)
        if not depo_addr:
            self._log_trade_event(trade_log_entry, f"Failed to get deposit address for {base_asset} on {opp.sell_exchange_id} via network {current_chosen_network_name}.", is_error=True)
            trade_log_entry.status = "TRANSFER_LEG_FAILED_DEPOSIT_ADDRESS"
            self.active_trades.discard(trade_key); return False
        
        if await self.rebalancer._execute_withdrawal_generic(buy_ex, opp.buy_exchange_id, base_asset, actual_amount_bought_base_net_fee, current_chosen_network_details, depo_addr):
            transfer_initiated_successfully = True
            used_network_details = current_chosen_network_details
        else:
            self._log_trade_event(trade_log_entry, f"Withdrawal of {base_asset} from {opp.buy_exchange_id} via network {current_chosen_network_name} failed.", is_error=True)
            trade_log_entry.status = "TRANSFER_LEG_FAILED_WITHDRAWAL_EXEC"
            self.active_trades.discard(trade_key); return False

        trade_log_entry.transfer_leg_details = {
            'from_exchange': opp.buy_exchange_id, 'to_exchange': opp.sell_exchange_id,
            'asset': base_asset, 'amount_sent_gross': str(actual_amount_bought_base_net_fee), 
            'network': opp.chosen_network, 
            'fee_native_estimate': str(used_network_details.get('fee_native') if used_network_details else "N/A"),
            'fee_currency_estimate': used_network_details.get('fee_currency') if used_network_details else "N/A"
        }
        trade_log_entry.status = "TRANSFER_LEG_INITIATED_WAITING_ARRIVAL"
        self._log_trade_event(trade_log_entry, f"Transfer of {base_asset} initiated via network {opp.chosen_network}.")

        amount_subject_to_network_fee = actual_amount_bought_base_net_fee 
        
        withdrawal_fee_native = used_network_details.get('fee_native', Decimal(0)) if used_network_details else Decimal(0)
        withdrawal_fee_currency = (used_network_details.get('fee_currency', base_asset) if used_network_details else base_asset).upper()

        expected_arrival_amount_net_of_wd_fee = amount_subject_to_network_fee
        if withdrawal_fee_currency == base_asset.upper(): 
            expected_arrival_amount_net_of_wd_fee -= withdrawal_fee_native
        
        sell_ex_arrival_check_params = self.rebalancer._get_account_params(opp.sell_exchange_id, "withdrawal") 
        
        amount_arrived_total_on_account = await self._wait_for_asset_arrival(
            opp.sell_exchange_id, sell_ex, base_asset, 
            expected_arrival_amount_net_of_wd_fee, 
            sell_ex_arrival_check_params, trade_log_entry, Config.BASE_ASSET_TRANSFER_WAIT_S
        )

        if amount_arrived_total_on_account is None: 
            trade_log_entry.status = "TRANSFER_LEG_FAILED_NOT_ARRIVED"
            self.active_trades.discard(trade_key); return False
        
        trade_log_entry.base_asset_received_on_sell_exchange = amount_arrived_total_on_account

        # --- SELL LEG ---
        trade_log_entry.status = "SELL_LEG_PENDING"
        self._log_trade_event(trade_log_entry, f"Stage 3: Preparing to sell base asset on {opp.sell_exchange_id}. Amount on arrival account: {amount_arrived_total_on_account:.8f} {base_asset}")

        if not await self.rebalancer._internal_transfer_if_needed(opp.sell_exchange_id, base_asset, amount_arrived_total_on_account, "withdrawal", "trading", all_balances_snapshot):
            self._log_trade_event(trade_log_entry, f"Failed internal transfer of {base_asset} to trading account on {opp.sell_exchange_id} before sell.", is_error=True)
            trade_log_entry.status = "SELL_LEG_FAILED_INTERNAL_TRANSFER_BASE"
            self.active_trades.discard(trade_key); return False

        sell_ex_trading_params = self.rebalancer._get_account_params(opp.sell_exchange_id, "trading")
        amount_to_sell_final_on_trading = await self.balance_manager.get_specific_account_balance(sell_ex, base_asset, "trading account pre-sell", sell_ex_trading_params)

        if amount_to_sell_final_on_trading is None or amount_to_sell_final_on_trading <= DECIMAL_COMPARISON_EPSILON:
            self._log_trade_event(trade_log_entry, f"No {base_asset} or zero amount ({amount_to_sell_final_on_trading if amount_to_sell_final_on_trading is not None else 'Error'}) on trading account of {opp.sell_exchange_id} before sell. Expected ~{amount_arrived_total_on_account:.8f}.", is_error=True)
            trade_log_entry.status = "SELL_LEG_FAILED_BALANCE_PRE_SELL"
            self.active_trades.discard(trade_key); return False

        self._log_trade_event(trade_log_entry, f"Actual amount available for selling on trading account of {opp.sell_exchange_id}: {amount_to_sell_final_on_trading:.8f} {base_asset}")

        sell_order_id = None; usdt_received_net = None; sell_avg_price = None; sell_fee_parsed = None; sell_status_parsed = "UNKNOWN"; raw_sell_response = None
        
        try:
            market_sell = sell_ex.markets.get(opp.symbol, {})
            if not market_sell: raise ValueError(f"Market {opp.symbol} not found on {opp.sell_exchange_id}")

            min_sell_amount_str = market_sell.get('limits', {}).get('amount', {}).get('min')
            min_sell_amount = Decimal(str(min_sell_amount_str)) if min_sell_amount_str is not None else Decimal('0')

            amount_to_sell_precise_str = sell_ex.amount_to_precision(opp.symbol, amount_to_sell_final_on_trading)
            amount_for_sell_order_call = Decimal(amount_to_sell_precise_str)

            if amount_for_sell_order_call < min_sell_amount and min_sell_amount > 0:
                raise ValueError(f"Amount to sell {amount_for_sell_order_call} {base_asset} for {opp.symbol} is less than exchange minimum {min_sell_amount}. Original amount on trading: {amount_to_sell_final_on_trading:.8f}")
            if amount_for_sell_order_call <= DECIMAL_COMPARISON_EPSILON:
                raise ValueError(f"Amount to sell for {opp.symbol} is zero or negligible after adjustments. Original amount on trading: {amount_to_sell_final_on_trading:.8f}")

            order_params_sell = {} 
            if sell_ex.id == 'kucoin' and Config.API_KEYS.get('kucoin',{}).get('password'):
                 order_params_sell = {'tradingPassword': Config.API_KEYS['kucoin']['password']}
            
            initial_sell_response: Optional[Dict[str, Any]] = None
            if Config.DRY_RUN:
                self.logger.info(f"[DRY RUN] Would place market SELL for {amount_for_sell_order_call} {base_asset} of {opp.symbol} on {opp.sell_exchange_id}")
                raw_sell_response = { 
                    'id': f'dryrun_sell_{int(time.time())}', 'symbol': opp.symbol, 'status': 'closed',
                    'filled': float(amount_for_sell_order_call), 'cost': float(amount_for_sell_order_call * opp.sell_price),
                    'average': float(opp.sell_price), 'timestamp': int(time.time() * 1000),
                    'fee': {'cost': float(amount_for_sell_order_call * opp.sell_price * (opp.sell_fee_pct or Decimal('0.1')) / Decimal('100')), 'currency': quote_asset} if opp.sell_fee_pct else None
                }
            else: 
                self.logger.info(f"Executing LIVE market SELL for {amount_for_sell_order_call} {base_asset} of {opp.symbol} on {opp.sell_exchange_id}.")
                initial_sell_response = await sell_ex.create_market_sell_order(opp.symbol, float(amount_for_sell_order_call), params=order_params_sell)
                self.logger.info(f"Initial sell order response: {json.dumps(initial_sell_response, indent=2, default=str) if initial_sell_response else 'No response'}")

                if not initial_sell_response or not initial_sell_response.get('id'): 
                    if initial_sell_response and str(initial_sell_response.get('status','')).lower() == 'closed' and \
                       Decimal(str(initial_sell_response.get('filled','0'))) > DECIMAL_COMPARISON_EPSILON: 
                        self.logger.warning(f"Using initial response for sell order {opp.symbol} as final due to missing ID but closed status.")
                        raw_sell_response = initial_sell_response
                    else:
                        raise ccxt_async.ExchangeError("No order ID in sell response and order not immediately closed.")

                if not raw_sell_response: 
                    sell_order_id = initial_sell_response['id']
                    await asyncio.sleep(3) 
                    raw_sell_response = await self._fetch_order_with_retry(sell_ex, sell_order_id, opp.symbol)
                
                if not raw_sell_response: 
                    raise ccxt_async.ExchangeError(f"Failed to fetch final details for sell order {sell_order_id if sell_order_id else 'ID_UNKNOWN'}.")

            status_fetch_sell = raw_sell_response.get('status'); sell_status_parsed = str(status_fetch_sell).upper() if status_fetch_sell else "UNKNOWN"

            if sell_status_parsed != "CLOSED":
                self.logger.error(f"Sell order {sell_order_id or raw_sell_response.get('id')} did not close. Status: {sell_status_parsed}. Raw: {json.dumps(raw_sell_response, indent=2, default=str)}")
                if sell_status_parsed not in ["CANCELED", "REJECTED", "EXPIRED"] and sell_ex.has['cancelOrder'] and (sell_order_id or raw_sell_response.get('id')) and not Config.DRY_RUN:
                    try: 
                        order_id_to_cancel_sell = sell_order_id or raw_sell_response.get('id')
                        self.logger.warning(f"Attempting to cancel non-closed sell order {order_id_to_cancel_sell} for {opp.symbol}.")
                        await sell_ex.cancel_order(order_id_to_cancel_sell, opp.symbol)
                    except Exception as e_cancel_sell:
                        self.logger.error(f"Failed to cancel non-closed sell order {order_id_to_cancel_sell}: {e_cancel_sell}")
                raise OrderNotClosedError(f"Sell order {sell_order_id or raw_sell_response.get('id')} status: {sell_status_parsed}")

            filled_sell_str = raw_sell_response.get('filled'); cost_sell_str = raw_sell_response.get('cost'); avg_price_sell_str = raw_sell_response.get('average')
            
            filled_sell_dec = Decimal(str(filled_sell_str)) if filled_sell_str is not None and Decimal(str(filled_sell_str)) > DECIMAL_COMPARISON_EPSILON else None
            if filled_sell_dec is None and sell_status_parsed == 'CLOSED': 
                amount_sell_str = raw_sell_response.get('amount')
                if amount_sell_str is not None and Decimal(str(amount_sell_str)) > DECIMAL_COMPARISON_EPSILON:
                    filled_sell_dec = Decimal(str(amount_sell_str))
                    self.logger.info(f"Used 'amount' as filled for closed sell order {sell_order_id or raw_sell_response.get('id')}.")

            if filled_sell_dec is None: 
                raise OrderNotClosedError(f"Sell order {sell_order_id or raw_sell_response.get('id')} not filled or filled amount is zero. Status: {sell_status_parsed}. Raw: {json.dumps(raw_sell_response, indent=2, default=str)}")

            usdt_gross = Decimal(str(cost_sell_str)) if cost_sell_str and Decimal(str(cost_sell_str)) > 0 else \
                         (filled_sell_dec * Decimal(str(avg_price_sell_str)) if avg_price_sell_str and Decimal(str(avg_price_sell_str)) > 0 and filled_sell_dec else \
                         (filled_sell_dec * opp.sell_price if filled_sell_dec else Decimal(0))) 
            sell_avg_price = Decimal(str(avg_price_sell_str)) if avg_price_sell_str and Decimal(str(avg_price_sell_str)) > 0 else \
                             (usdt_gross / filled_sell_dec if filled_sell_dec and usdt_gross > 0 and filled_sell_dec > 0 else opp.sell_price)

            usdt_received_net = usdt_gross
            sell_fees_list = raw_sell_response.get('fees', [])
            if not sell_fees_list and raw_sell_response.get('fee'): sell_fees_list = [raw_sell_response.get('fee')]
            
            if sell_fees_list and isinstance(sell_fees_list[0], dict):
                fee_info_sell = sell_fees_list[0]
                if fee_info_sell.get('cost') is not None and fee_info_sell.get('currency') is not None:
                    sell_fee_parsed = {'currency': fee_info_sell['currency'].upper(), 'cost': Decimal(str(fee_info_sell['cost']))}
                    if sell_fee_parsed['currency'] == quote_asset.upper(): 
                        usdt_received_net = usdt_gross - sell_fee_parsed['cost']
            
            if not usdt_received_net or usdt_received_net <= DECIMAL_COMPARISON_EPSILON:
                raise ValueError(f"Sell order resulted in zero or negligible {quote_asset} after fees.")

            self.logger.info(f"Sell Leg Final: Net USDT Received {usdt_received_net:.2f}, Avg Price {sell_avg_price:.8f}")
            trade_log_entry.sell_leg = TradeExecutionDetails(
                order_id=sell_order_id or raw_sell_response.get('id'), timestamp=raw_sell_response.get('timestamp', int(time.time()*1000)),
                symbol=opp.symbol, side='sell', price=sell_avg_price,
                amount_base=filled_sell_dec, cost_quote=usdt_received_net, 
                fee_amount=sell_fee_parsed.get('cost') if sell_fee_parsed else None,
                fee_currency=sell_fee_parsed.get('currency') if sell_fee_parsed else None,
                status=sell_status_parsed, raw_response=raw_sell_response
            )
            trade_log_entry.usdt_received_from_sell = usdt_received_net

            if trade_log_entry.initial_buy_cost_usdt is not None:
                trade_log_entry.final_net_profit_usdt = usdt_received_net - trade_log_entry.initial_buy_cost_usdt
                if trade_log_entry.initial_buy_cost_usdt > 0: 
                    trade_log_entry.final_net_profit_pct = (trade_log_entry.final_net_profit_usdt / trade_log_entry.initial_buy_cost_usdt) * Decimal('100')
                
                profit_log_msg = f"Initial Cost: {trade_log_entry.initial_buy_cost_usdt:.2f} | Final Received: {usdt_received_net:.2f} | Net Profit: {trade_log_entry.final_net_profit_usdt:.2f} USDT ({trade_log_entry.final_net_profit_pct if trade_log_entry.final_net_profit_pct is not None else 'N/A':.3f}%)"
                if trade_log_entry.final_net_profit_usdt > 0:
                    self.logger.info(f"🎉 ARBITRAGE COMPLETED (PROFIT): {trade_key} 🎉 | {profit_log_msg}")
                    trade_log_entry.status = "COMPLETED_SUCCESS"
                else:
                    self.logger.info(f"💔 ARBITRAGE COMPLETED (LOSS/BREAKEVEN): {trade_key} 💔 | {profit_log_msg}")
                    trade_log_entry.status = "COMPLETED_LOSS"
            else: 
                self.logger.warning("Could not calculate final profit due to missing initial_buy_cost_usdt.");
                trade_log_entry.status = "COMPLETED_UNKNOWN_PROFIT"

        except (ccxt_async.InsufficientFunds, ccxt_async.InvalidOrder, ccxt_async.ExchangeError, ValueError, OrderNotClosedError) as e_sell:
            self._log_trade_event(trade_log_entry, f"Error in sell leg: {type(e_sell).__name__} - {e_sell}",is_error=True,exc_info_dump=True)
            trade_log_entry.status="SELL_LEG_FAILED"
            self.active_trades.discard(trade_key); return False
        except Exception as e_unexp_sell: 
            self._log_trade_event(trade_log_entry, f"Unexpected sell error: {type(e_unexp_sell).__name__} - {e_unexp_sell}",is_error=True,exc_info_dump=True)
            trade_log_entry.status="SELL_LEG_FAILED_UNEXPECTED"
            self.active_trades.discard(trade_key); return False
        finally:
            self.active_trades.discard(trade_key) 

        return True


async def main_loop():
    logger.info("--- Arbitrage Bot Starting Main Loop ---")
    Config.load_exchange_fees_from_file()
    # The setup_arbitrage_rules() call is removed, as rules are now static class attributes.
    logger.info(f"Loaded {len(Config.ARB_WHITELIST)} whitelist paths and {len(Config.ARB_PATH_BLACKLIST)} specific blacklist paths.")


    active_exchanges: Dict[str, ccxt_async.Exchange] = {}
    for ex_id, creds in Config.API_KEYS.items():
        if not creds.get('apiKey') or not creds.get('secret'):
            logger.warning(f"API key or secret missing for {ex_id}. Skipping.")
            continue
        if not Config.DRY_RUN and ("YOUR_API_KEY" in creds['apiKey'].upper() or "YOUR_SECRET" in creds['secret'].upper()):
            logger.critical(f"Placeholder API keys found for {ex_id}. Bot will not run with placeholder keys when DRY_RUN is False. Exiting.")
            return

        exchange_class = getattr(ccxt_async, ex_id, None)
        if not exchange_class:
            logger.warning(f"Exchange class for '{ex_id}' not found in ccxt.async_support. Skipping.")
            continue
        try:
            common_options = {'enableRateLimit': True, 'timeout': 30000}
            exchange_config = {**creds, **common_options}
            if 'options' in creds and isinstance(creds['options'], dict):
                if 'options' not in exchange_config or not isinstance(exchange_config['options'], dict):
                    exchange_config['options'] = {}
                exchange_config['options'].update(creds['options'])

            active_exchanges[ex_id] = exchange_class(exchange_config)
            logger.info(f"Initialized exchange: {ex_id}")
        except Exception as e:
            logger.error(f"Failed to initialize exchange {ex_id}: {e}", exc_info=True)

    if len(active_exchanges) < 2:
        logger.critical(f"Need at least 2 successfully initialized exchanges to run. Found {len(active_exchanges)}. Exiting.")
        for ex_id_close, ex_inst_close in active_exchanges.items():
            if ex_inst_close:
                try: await ex_inst_close.close()
                except: pass
        return

    balance_manager = BalanceManager(active_exchanges)
    scanner = Scanner(active_exchanges)
    analyzer = Analyzer(active_exchanges)
    analyzer.set_balance_manager(balance_manager)
    rebalancer = Rebalancer(active_exchanges, balance_manager, analyzer)
    executor = Executor(active_exchanges, balance_manager, analyzer, rebalancer)
    analyzer.set_executor(executor)

    logger.info("Initializing Scanner (loading markets, finding common pairs)...")
    await scanner.initialize()
    if not scanner.common_pairs:
        logger.critical("Scanner initialization failed to find any common pairs. Bot cannot proceed. Exiting.")
        for ex_id_close, ex_inst_close in active_exchanges.items():
            if ex_inst_close:
                try: await ex_inst_close.close()
                except: pass
        return

    logger.info("Performing initial caching of currency information for all exchanges...")
    await analyzer._cache_all_currencies_info_for_enrichment([])


    for cycle_num in range(1, Config.TRADE_CYCLE_COUNT + 1 if Config.TRADE_CYCLE_COUNT > 0 else sys.maxsize):
        logger.info(f"========= Arbitrage Cycle {cycle_num}/{Config.TRADE_CYCLE_COUNT if Config.TRADE_CYCLE_COUNT > 0 else 'Infinite'} =========")
        current_all_balances = None
        try:
            if cycle_num > 1 and cycle_num % 10 == 0:
                stale_cleaned = rebalancer.cleanup_stale_operations(max_age_seconds=7200)
                if stale_cleaned > 0:
                    logger.info(f"Cleaned up {stale_cleaned} stale rebalance operations.")

            logger.info("Fetching current balances for all exchanges...")
            current_all_balances = await balance_manager.get_all_balances(calculate_usd_values=True)
            if not current_all_balances:
                logger.error("Failed to fetch any balances. Skipping cycle actions.")
                await asyncio.sleep(Config.TRADE_CYCLE_SLEEP_S); continue

            logger.info("Scanning for arbitrage opportunities...")
            gross_opportunities = await scanner.scan_once()
            if not gross_opportunities:
                logger.info("No gross opportunities found in this scan cycle.")
            else:
                logger.info(f"Found {len(gross_opportunities)} gross opportunities. Analyzing for stability and net profit...")
                best_opportunity = await analyzer.analyze_and_select_best(gross_opportunities)
                if best_opportunity:
                    logger.info(f"Selected best opportunity: {best_opportunity.get_unique_id()} with net profit {best_opportunity.net_profit_pct if best_opportunity.net_profit_pct is not None else 'N/A':.3f}% and liquidity status: {best_opportunity.is_liquid_enough_for_trade}")
                    
                    # Final check for hardcoded unavailable assets (belt and suspenders)
                    if (best_opportunity.buy_exchange_id, best_opportunity.base_asset) in Config.ASSET_UNAVAILABLE_BLACKLIST or \
                       (best_opportunity.sell_exchange_id, best_opportunity.base_asset) in Config.ASSET_UNAVAILABLE_BLACKLIST:
                        logger.warning(f"Skipping execution of {best_opportunity.get_unique_id()} because it involves an asset on the ASSET_UNAVAILABLE_BLACKLIST.")
                    elif best_opportunity.is_liquid_enough_for_trade:
                        if Config.DRY_RUN:
                            logger.info(f"[DRY RUN MODE] Would attempt to execute trade for: {best_opportunity.get_unique_id()}")
                            await executor.execute_arbitrage(best_opportunity, current_all_balances)
                        else:
                            logger.info(f"Attempting to execute LIVE trade for: {best_opportunity.get_unique_id()}")
                            trade_success = await executor.execute_arbitrage(best_opportunity, current_all_balances)
                            if trade_success:
                                logger.info(f"Trade execution for {best_opportunity.get_unique_id()} reported success. Cooldown period starting.")
                                await asyncio.sleep(Config.POST_TRADE_COOLDOWN_S)
                                logger.info("Fetching balances after trade and cooldown...")
                                current_all_balances = await balance_manager.get_all_balances(calculate_usd_values=True)
                                if not current_all_balances:
                                    logger.error("Failed to fetch balances after trade. Subsequent logic might use stale data.")
                            else:
                                logger.warning(f"Trade execution for {best_opportunity.get_unique_id()} reported failure or no action taken.")
                    else:
                        logger.warning(f"Skipping execution of {best_opportunity.get_unique_id()} due to insufficient liquidity reported by final check in Analyzer.")
                else:
                    logger.info("No suitable net profitable and stable opportunity selected after analysis.")

            if cycle_num % Config.ASSET_CONSOLIDATION_INTERVAL_CYCLES == 0:
                logger.info(f"Performing periodic asset consolidation (every {Config.ASSET_CONSOLIDATION_INTERVAL_CYCLES} cycles)...")
                if not current_all_balances:
                     current_all_balances = await balance_manager.get_all_balances(calculate_usd_values=True)

                if current_all_balances:
                    for ex_id_consolidate, ex_bal_consolidate in current_all_balances.items():
                        if ex_bal_consolidate and ex_bal_consolidate.assets:
                            await rebalancer.convert_assets_to_usdt_on_exchange(
                                ex_id_consolidate,
                                ex_bal_consolidate,
                                min_usd_value=Config.MIN_EFFECTIVE_TRADE_USD,
                                executor_instance=executor
                            )
                    logger.info("Periodic asset consolidation attempts finished.")
                    logger.info("Fetching balances after asset consolidation attempts...")
                    current_all_balances = await balance_manager.get_all_balances(calculate_usd_values=True)
                    if not current_all_balances:  logger.error("Failed to fetch balances post-consolidation.")
                else:
                    logger.warning("Could not fetch balances for periodic asset consolidation.")

        except Exception as e_cycle:
            logger.error(f"Unhandled error in arbitrage cycle {cycle_num}: {e_cycle}", exc_info=True)

        if Config.TRADE_CYCLE_COUNT > 0 and cycle_num >= Config.TRADE_CYCLE_COUNT:
            break

        logger.info(f"End of cycle {cycle_num}. Sleeping for {Config.TRADE_CYCLE_SLEEP_S} seconds...")
        await asyncio.sleep(Config.TRADE_CYCLE_SLEEP_S)

    logger.info("All configured arbitrage cycles completed or bot stopped.")
    logger.info("Closing all exchange connections...")
    for ex_id, ex_inst in active_exchanges.items():
        if ex_inst:
            try:
                await ex_inst.close()
                logger.info(f"Closed connection for {ex_id}.")
            except Exception as e_close:
                logger.error(f"Error closing connection for {ex_id}: {e_close}")

    if executor.trade_history:
        try:
            history_file_path = "arbitrage_trade_history_full_cycle.jsonl"
            with open(history_file_path, 'w', encoding='utf-8') as f_hist:
                for entry in executor.trade_history:
                    entry_dict = {}
                    slots_or_dict = entry.__dict__ if hasattr(entry, '__dict__') else {slot: getattr(entry, slot) for slot in getattr(entry, '__slots__', [])}
                    
                    for slot_name, val in slots_or_dict.items():
                        if isinstance(val, Decimal):
                            entry_dict[slot_name] = str(val)
                        elif isinstance(val, TradeExecutionDetails) and val is not None:
                            detail_dict = {}
                            detail_slots_or_dict_inner = val.__dict__ if hasattr(val, '__dict__') else {slot: getattr(val, slot) for slot in getattr(val, '__slots__', [])}
                            for detail_slot_name, detail_val in detail_slots_or_dict_inner.items():
                                if isinstance(detail_val, Decimal):
                                    detail_dict[detail_slot_name] = str(detail_val)
                                elif isinstance(detail_val, dict) and detail_slot_name == 'raw_response':
                                     detail_dict[detail_slot_name] = {"type": str(type(detail_val)), "keys_sample": list(detail_val.keys())[:5]} if detail_val else None
                                else:
                                    detail_dict[detail_slot_name] = detail_val
                            entry_dict[slot_name] = detail_dict
                        elif isinstance(val, dict) and slot_name == 'original_opportunity_details':
                             entry_dict[slot_name] = {k_opp: (str(v_opp) if isinstance(v_opp, Decimal) else v_opp) for k_opp, v_opp in val.items()}
                        elif isinstance(val, dict) and slot_name == 'transfer_leg_details':
                             entry_dict[slot_name] = {k_trans: (str(v_trans) if isinstance(v_trans, Decimal) else v_trans) for k_trans, v_trans in val.items()}
                        else:
                            entry_dict[slot_name] = val
                    json.dump(entry_dict, f_hist, ensure_ascii=False)
                    f_hist.write('\n')
            logger.info(f"Trade history saved to {history_file_path}")
        except Exception as e_save_hist:
            logger.error(f"Error saving trade history: {e_save_hist}", exc_info=True)


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Bot interrupted by user (KeyboardInterrupt). Shutting down...")
    except Exception as e_main:
        logger.critical(f"Critical unhandled exception in main: {e_main}", exc_info=True)
    finally:
        logger.info("Bot shutdown complete.")