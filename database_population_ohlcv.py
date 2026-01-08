import sqlite3
import time
from datetime import datetime
import logging
import csv
from ibapi.client import EClient
from ibapi.common import BarData
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper
from threading import Thread


engine_name = ''


def convert_date_to_iso(datetime_str):
    """
    Convert IB API date format to ISO format.

    Args:
        datetime_str (str): Date string in format '20250414 15:30:00 Europe/Berlin'

    Returns:
        str: Date string in ISO format '2025-04-14 15:30:00' or None if conversion fails
    """

    try:
        datetime_str = datetime_str.strip()
        parts = datetime_str.split(' ')
        parts = [part for part in parts if part != '']
        if len(parts) < 2:
            raise ValueError(f"Input date string {datetime_str} is missing the required date and time parts.")

        date_part = parts[0]
        date_obj = datetime.strptime(date_part, '%Y%m%d')
        iso_date = date_obj.strftime('%Y-%m-%d')
        time_part = parts[1]
        ret = f"{iso_date} {time_part}"
        #check format of ret
        if datetime.strptime(ret, '%Y-%m-%d %H:%M:%S') is None:
            raise ValueError(f"Converted date string {ret} is not in the expected ISO format.")
        return ret

    except Exception as e:
        logging.error("Error converting date: %s", e)
        return None


def convert_date_to_ib_format(end_date):
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    end_date = end_date_obj.strftime('%Y%m%d %H:%M:%S')
    return end_date


class IBClient(EClient, EWrapper):

    def __init__(self, host, port, client_id, symbol_list, insert_to_db=True):
        EClient.__init__(self, self)
        self.order_id = 0
        self.bars = {}
        self.symbol_list = symbol_list
        self.insert_to_db = insert_to_db
        self.last_update = time.monotonic()
        self.completion_status = {} #to track completion status of each stock request(marked complete either on data received or error)
        self.connect(host, port, client_id)
        self.symbols_to_blacklist = [] #for storing symbols which need to be blacklisted (200, definition not found)
        self.failed_to_insert_symbols = [] #for storing symbols which failed to insert into DB
        self.erroneous_symbols = [] #for storing symbols which had errors during data fetching to review later
        thread = Thread(target=self.run)
        thread.start()
        time.sleep(1)

    def nextValidId(self, order_id):
        super().nextValidId(order_id)
        self.order_id = order_id

    # use this to fetch subsequent valid IDs
    def nextId(self):
        self.order_id += 1
        return self.order_id

    def error(self, req_id, code, msg, misc=''):

        if code in [2104, 2106, 2158, 2174, 2176]:
            logging.warn("ReqID: %s, Error Code: %s, Message: %s", req_id, code, msg)
        else:
            logging.error("ReqID: %s - Error Code: %s, Message: %s", req_id, code, msg)
            # errors:
            # 200: definition not found (stock doesnt exist anymore) -> blacklist, mark request complete
            # 162?
            # all other errors?

            if code in [162, 200]: #definition not found (stock doesnt exist anymore) -> blacklist and mark request complete
                self.mark_stocks_for_blacklisting([req_id])
            else:
                self.mark_stock_as_erroneous(code, msg, req_id) # all other errors -> flag as erroneous for review and mark request complete

            self.mark_request_completion(req_id)


    def mark_stock_as_erroneous(self, code, msg, req_id):
        if self.completion_status.get(req_id, None) is not None:
            self.erroneous_symbols.append((req_id, code, msg))

    def mark_stocks_for_blacklisting(self, stock_ids):
        self.symbols_to_blacklist.extend(stock_ids)

    def reset_timeout_timer(self):
        self.last_update = time.monotonic()

    def mark_request_completion(self, req_id):
        if self.completion_status.get(req_id, None) is not None:
            self.completion_status[req_id] = True
            self.reset_timeout_timer()



    def historicalData(self, req_id: int, bar: BarData):
        data = {
            'date': convert_date_to_iso(bar.date),
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': int(bar.volume)
        }

        try:
            self.validate_bar_data(data)
        except ValueError as ve:
            logging.error("ReqID: %s - Data validation error - Timestamp %s: %s", req_id, bar.date, str(ve))
            return

        self.bars[req_id].append(data)

    @staticmethod
    def validate_bar_data(data):
        if data['date'] is None:
            raise ValueError(f"Invalid date format received")
        if data['open'] <= 0 or data['high'] <= 0 or data['low'] <= 0 or data['close'] <= 0 or data['volume'] < 0:
            raise ValueError(f"Invalid OHLCV values received: O:{data['open']} H:{data['high']} L:{data['low']} C:{data['close']} V:{data['volume']}")
        if data['high'] < data['low'] or data['high'] < data['open'] or data['high'] < data['close']:
            raise ValueError(f"Inconsistent high value: H:{data['high']} L:{data['low']} O:{data['open']} C:{data['close']}")
        if data['low'] > data['high'] or data['low'] > data['open'] or data['low'] > data['close']:
            raise ValueError(f"Inconsistent low value: L:{data['low']} H:{data['high']} O:{data['open']} C:{data['close']}")
        if data['open'] is None or data['high'] is None or data['low'] is None or data['close'] is None or data['volume'] is None:
            raise ValueError(f"Missing OHLCV values received: O:{data['open']} H:{data['high']} L:{data['low']} C:{data['close']} V:{data['volume']}")

    def historicalDataEnd(self, req_id: int, start: str, end: str):
        logging.info(f"End of data - ReqID: %s. Start: %s. End: %s", req_id, start, end)

        if len(self.bars[req_id]) == 0:
            logging.warning("ReqID: %s. No valid data received for writing to database.", req_id)

        if self.insert_to_db and len(self.bars[req_id]) > 0:
            conn = sqlite3.connect(engine_name)
            cursor = conn.cursor()

            stock_id = req_id

            data_to_insert = [
                (stock_id, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'])
                for row in self.bars[req_id]
            ]

            try:
                cursor.execute("BEGIN")
                cursor.executemany(
                    "INSERT OR IGNORE INTO stock_data_5m (stock_id, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    data_to_insert
                )
                conn.commit()
                logging.info("ReqID: %s. Data inserted successfully.", req_id)
            except Exception as e:
                logging.error("Error inserting data: %s", e)
                conn.rollback()
                self.failed_to_insert_symbols.append(stock_id)

            conn.close()

        self.bars[req_id].clear()
        self.mark_request_completion(req_id)


    def fetch_historical_data(self, end_date, time_period, bar_size):
        end_date = convert_date_to_ib_format(end_date)
        for id, symbol in self.symbol_list:
            self.completion_status[id] = False
            contract = Contract()
            contract.symbol = symbol
            contract.secType = 'STK'
            contract.exchange = 'SMART'
            contract.currency = 'USD'
            what_to_show = 'TRADES'

            self.bars[id] = []
            self.reqHistoricalData(
                id, contract, end_date, time_period, bar_size, what_to_show, True, 1, False, []
            )


def get_symbols_from_db(is_blacklisted=0, new_stocks_only=None):
    # get all non-blacklisted symbols from our "stocks" table
    symbols = []
    db_conn = sqlite3.connect(engine_name)
    db_cursor = db_conn.cursor()
    query = f"SELECT id, symbol FROM stocks where is_blacklisted = {is_blacklisted}"

    if new_stocks_only:
        query += f" and modified_at > '2025-01-01 00:00:00'"
    db_cursor.execute(query)

    for row in db_cursor.fetchall():
        symbols.append((row[0], row[1]))

    db_conn.close()
    return symbols

def get_symbols_from_db_by_name(symbols_to_get):
    #get specific symbols by name
    symbols = []
    db_conn = sqlite3.connect(engine_name)
    db_cursor = db_conn.cursor()
    query = f"""
    SELECT id, symbol FROM stocks
    where is_blacklisted = 0
    and symbol in ({','.join(['?']*len(symbols_to_get))}) """
    db_cursor.execute(query, symbols_to_get)

    for row in db_cursor.fetchall():
        symbols.append((row[0], row[1]))

    db_conn.close()
    return symbols


def get_symbols_from_db_by_id(symbols_to_get):
    #get specific symbols by name
    symbols = []
    db_conn = sqlite3.connect(engine_name)
    db_cursor = db_conn.cursor()
    query = f"""
    SELECT id, symbol FROM stocks
    where is_blacklisted = 0
    and id in ({','.join(['?']*len(symbols_to_get))}) """
    db_cursor.execute(query, symbols_to_get)

    for row in db_cursor.fetchall():
        symbols.append((row[0], row[1]))

    db_conn.close()
    return symbols


def blacklist_stocks(symbols_to_blacklist, all_symbols, write_to_db=True):
    if write_to_db and symbols_to_blacklist:
        db_conn = sqlite3.connect(engine_name)
        db_cursor = db_conn.cursor()
        query = f"""
            Update stocks
            set is_blacklisted = 1
            where id in ({','.join(['?']*len(symbols_to_blacklist))})
        """
        db_cursor.execute(query, symbols_to_blacklist)
        db_conn.commit()
        db_conn.close()

    logging.info("Blacklisted %s stocks: %s", len(symbols_to_blacklist), symbols_to_blacklist)

#########################################


#### PARAMETERS ####

## Change this to False if you want to get all stocks
## True will get only get the new stocks that we added recently)
new_stocks_only = False

## Change this to True if you want to write fetched data to the database
## Set to False if you just want to test fetching data without writing to DB (did this for debugging purposes)
write_to_db = True

## Timeout duration in seconds. Time to wait after last data received before marking remaining stocks as failed
## Resets every time data is received for any stock (or an error occurs)
timeout_duration = 60 * 30  # 30 minutes timeout

#Parameters for date, duration, bar granularity
end_date_str = '2025-12-02 23:00:00'
time_period = '21 D'
bar_size = '5 mins'


#####################

#creates a log file on every run, named with the timestamp
log_filename = f"logs/db_population_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging
logging.basicConfig(
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

#symbols = get_symbols_from_db(is_blacklisted=0, new_stocks_only=new_stocks_only)
#symbols = get_symbols_from_db_by_name(symbols_to_get=['TORO', 'TVGN', 'VIRC'])
symbols = get_symbols_from_db_by_id(symbols_to_get=[1674, 1704, 1760, 1791, 1799, 1842, 1860, 1867, 1868, 1869, 1878, 1900, 1902, 1903, 1911, 1916, 1919, 1922, 1923, 1943, 1944, 1945, 1947, 1949, 1953, 1956, 1959, 1962, 1972, 1975, 1976, 1977, 1980, 1981, 1983, 1984, 1985, 1986, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035, 2036, 2037, 2038, 2039, 2040, 2041, 2042, 2044, 2045, 2046, 2047, 2048, 2049, 2050, 2051, 2052, 2053, 2055, 2056, 2057, 2058, 2059, 2060, 2061, 2062, 2063, 2064, 2065, 2066, 2067, 2068, 2069, 2070, 2071, 2072, 2073, 2074, 2075, 2076, 2077, 2078, 2079, 2080, 2081, 2082, 2083, 2084, 2085, 2086, 2087, 2089, 2091, 2092, 2094, 2095, 2096, 2097, 2098, 2099, 2100, 2101, 2102, 2103, 2104, 2105, 2106, 2107, 2108, 2109, 2111, 2112, 2113, 2114, 2115, 2116, 2118, 2119, 2120, 2122, 2123, 2124, 2125, 2126, 2127, 2128, 2129, 2130, 2131, 2132, 2133, 2134, 2135, 2136, 2137, 2138, 2139, 2140, 2141, 2142, 2143, 2144, 2145, 2146, 2147, 2148, 2149, 2150, 2152, 2153, 2154, 2155, 2156, 2157, 2158, 2159, 2160, 2161, 2162, 2163, 2164, 2165, 2166, 2167, 2168, 2169, 2170, 2171, 2172, 2173, 2174, 2175, 2176, 2177, 2178, 2179, 2180, 2181, 2182, 2183, 2184, 2185, 2186, 2188, 2189, 2190, 2191, 2192, 2193, 2194, 2195, 2196, 2197, 2199, 2200, 2201, 2202, 2203, 2206, 2207, 2208, 2209, 2210, 2211, 2213, 2214, 2215, 2216, 2217, 2218, 2219, 2220, 2221, 2222, 2223, 2224, 2225, 2226, 2227, 2228, 2229, 2230, 2231, 2232, 2233, 2234, 2235, 2236, 2237, 2239, 2240, 2241, 2242, 2243, 2244, 2245, 2246, 2247, 2248, 2249, 2250, 2251, 2252, 2253, 2254, 2255, 2256, 2257, 2258, 2259, 2260, 2261, 2262, 2264, 2265, 2266, 2267, 2268, 2269, 2270, 2271, 2272, 2273, 2274, 2276, 2277, 2278, 2279, 2280, 2281, 2282, 2283, 2284, 2285, 2286, 2287, 2288, 2289, 2290, 2292, 2293, 2294, 2295, 2296, 2297, 2298, 2299, 2301, 2302, 2303, 2304, 2305, 2306, 2307, 2308, 2309, 2310, 2311, 2312, 2313, 2314, 2315, 2316, 2317, 2318, 2319, 2320, 2321, 2322, 2323, 2324, 2325, 2326, 2327, 2328, 2329, 2330, 2331, 2332, 2333, 2334, 2335, 2336, 2337, 2338, 2339, 2340, 2341, 2342, 2343, 2344, 2345, 2346, 2348, 2349, 2350, 2351, 2353, 2354, 2355, 2356, 2357, 2358, 2359, 2360, 2361, 2362, 2363, 2364, 2368, 2369, 2370, 2371, 2372, 2373, 2374, 2375, 2376, 2377, 2378, 2379, 2380, 2381, 2382, 2383, 2384, 2385, 2386, 2387, 2389, 2390, 2391, 2392, 2393, 2394, 2395, 2396, 2397, 2398, 2399, 2400, 2402, 2403, 2404, 2405, 2406, 2407, 2408, 2409, 2410, 2411, 2412, 2413, 2414, 2416, 2417, 2418, 2419, 2420, 2421, 2422, 2423, 2424, 2425, 2426, 2427, 2428, 2429, 2430, 2431, 2432, 2433, 2434, 2436, 2437, 2438, 2439, 2440, 2441, 2442, 2443, 2444, 2446, 2447, 2448, 2449, 2450, 2451, 2452, 2453, 2454, 2455, 2456, 2458, 2459, 2462, 2463, 2464, 2465, 2466, 2467, 2468, 2469, 2470, 2471, 2472, 2473, 2474, 2475, 2476, 2477, 2478, 2479, 2480, 2481, 2482, 2483, 2484, 2485, 2486, 2487, 2488, 2489, 2490, 2491, 2493, 2494, 2495, 2496, 2497, 2498, 2499, 2500, 2501, 2502, 2503, 2504, 2505, 2506, 2507, 2508, 2509, 2510, 2511, 2512, 2513, 2514, 2515, 2516, 2517, 2518, 2519, 2520, 2521, 2522, 2523, 2524, 2525, 2526, 2528, 2529, 2530, 2531, 2532, 2533, 2534, 2535, 2536, 2537, 2538, 2539, 2540, 2541, 2542, 2543, 2544, 2545, 2546, 2547, 2549, 2550, 2551, 2552, 2553, 2554, 2555, 2556, 2557, 2558, 2559, 2560, 2561, 2562, 2563, 2564, 2565, 2566, 2567, 2569, 2570, 2571, 2572, 2573, 2574, 2576, 2577, 2578, 2579, 2580, 2581, 2582, 2583, 2584, 2585, 2586, 2587, 2588, 2589, 2590, 2591, 2592, 2593, 2594, 2595, 2596, 2597, 2598, 2599, 2600, 2601, 2602, 2603, 2605, 2606, 2607, 2608, 2609, 2610, 2612, 2613, 2614, 2615, 2616, 2617, 2618, 2619, 2620, 2621, 2622, 2623, 2624, 2625, 2626, 2627, 2628, 2629, 2630, 2631, 2632, 2633, 2634, 2635, 2636, 2637, 2638, 2639, 2640, 2641, 2642, 2643, 2644, 2645, 2646, 2648, 2649, 2650, 2651, 2652, 2653, 2654, 2655, 2656, 2657, 2658, 2659, 2660, 2661, 2662, 2663, 2664, 2665, 2666, 2667, 2668, 2669, 2670, 2671, 2672, 2673, 2674, 2675, 2676, 2677, 2678, 2679, 2680, 2681, 2682, 2683, 2684, 2685, 2686, 2687, 2688, 2690, 2691, 2692, 2693, 2694, 2695, 2696, 2697, 2699, 2701, 2702, 2703, 2704, 2706, 2707, 2708, 2709, 2710, 2711, 2712, 2713, 2714, 2715, 2716, 2717, 2718, 2719, 2721, 2722, 2723, 2724, 2725, 2726, 2727, 2728, 2729, 2730, 2731, 2732, 2733, 2734, 2735, 2736, 2737, 2738, 2739, 2740, 2741, 2743, 2744, 2745, 2747, 2748, 2749, 2750, 2751, 2752, 2753, 2754, 2755, 2756, 2757, 2758, 2759, 2760, 2761, 2762, 2764, 2765, 2766, 2767, 2768, 2769, 2770, 2771, 2772, 2773, 2774, 2775, 2776, 2778, 2779, 2780, 2781, 2782, 2783, 2784, 2785, 2786, 2787, 2788, 2790, 2791, 2792, 2793, 2794, 2795, 2796, 2797, 2798, 2799, 2800, 2801, 2802, 2803, 2804, 2805, 2806, 2807, 2808, 2809, 2810, 2811, 2812, 2814, 2815, 2816, 2817, 2818, 2819, 2820, 2821, 2822, 2823, 2824, 2825, 2827, 2828, 2829, 2830, 2831, 2832, 2833, 2834, 2835, 2836, 2837, 2838, 2839, 2840, 2841, 2842, 2843, 2844, 2845, 2846, 2849, 2850, 2852, 2853, 2854, 2855, 2857, 2858, 2859, 2860, 2861, 2862, 2863, 2866, 2867, 2868, 2869, 2870, 2871, 2872, 2874, 2875, 2876, 2877, 2878, 2879, 2880, 2881, 2882, 2883, 2884, 2885, 2886, 2887, 2888, 2889, 2890, 2891, 2892, 2893, 2894, 2895, 2896, 2897, 2898, 2899, 2900, 2901, 2902, 2903, 2904, 2905, 2906, 2907, 2908, 2909, 2910, 2911, 2912, 2913, 2914, 2915, 2916, 2917, 2918, 2919, 2920, 2921, 2922, 2923, 2924, 2925, 2926, 2928, 2929, 2930, 2931, 2932, 2933, 2934, 2935, 2936, 2937, 2938, 2939, 2940, 2941, 2942, 2943, 2944, 2945, 2946, 2947, 2948, 2949, 2950, 2951, 2953, 2956, 2957, 2958, 2959, 2960, 2961, 2962, 2963, 2964, 2966, 2967, 2968, 2969, 2970, 2971, 2972, 2973, 2974, 2975, 2976, 2977, 2978, 2979, 2980, 2981, 2982, 2983, 2986, 2987, 2988, 2989, 2990, 2992, 2993, 2994, 2995, 2998, 2999, 3001, 3002, 3003, 3006, 3007, 3009, 3010, 3011, 3012, 3013, 3014, 3015, 3016, 3017, 3018, 3019, 3020, 3021, 3022, 3023, 3024, 3025, 3026, 3027, 3028, 3029, 3030, 3031, 3032, 3033, 3034, 3035, 3036, 3037, 3039, 3040, 3041, 3042, 3043, 3044, 3045, 3046, 3047, 3048, 3049, 3050, 3051, 3052, 3053, 3054, 3055, 3056, 3058, 3059, 3060, 3061, 3062, 3063, 3064, 3065, 3066, 3067, 3069, 3070, 3071, 3072, 3073, 3074, 3076, 3077, 3078, 3079, 3080, 3081, 3082, 3083, 3084, 3085, 3086, 3087, 3088, 3089, 3090, 3091, 3092, 3093, 3094, 3095, 3096, 3097, 3098, 3100, 3101, 3103, 3105, 3106, 3107, 3108, 3109, 3110, 3113, 3114, 3115, 3116, 3117, 3118, 3119, 3120, 3121, 3122, 3123, 3125, 3126, 3127, 3128, 3129, 3130, 3131, 3132, 3133, 3134, 3135, 3136, 3137, 3138, 3139, 3140, 3141, 3142, 3143, 3144, 3145, 3146, 3147, 3148, 3149, 3150, 3152, 3153, 3155, 3156, 3157, 3158, 3159, 3160, 3161, 3162, 3163, 3164, 3166, 3167, 3168, 3169, 3170, 3171, 3173, 3174, 3175, 3176, 3177, 3178, 3179, 3181, 3182, 3183, 3184, 3185, 3186, 3187, 3188, 3190, 3191, 3192, 3193, 3194, 3195, 3196, 3197, 3198, 3199, 3200, 3201, 3202, 3203, 3204, 3205, 3206, 3208, 3209, 3210, 3211, 3212, 3213, 3214, 3215, 3216, 3217, 3218, 3219, 3220, 3221, 3222, 3223, 3225, 3226, 3227, 3228, 3229, 3230, 3231, 3232, 3233, 3234, 3235, 3236, 3237, 3238, 3239, 3240, 3241, 3242, 3243, 3244, 3245, 3246, 3247, 3248, 3249, 3251, 3252, 3253, 3254, 3256, 3257, 3258, 3260, 3261, 3262, 3263, 3264, 3265, 3266, 3268, 3272, 3276, 3277, 3279, 3283, 3284, 3285, 3286, 3287, 3289, 3290, 3293, 3294, 3295, 3296, 3305, 3306, 3307, 3309, 3311, 3312, 3313, 3314, 3316, 3317, 3319, 3320, 3321, 3324, 3325, 3327, 3328, 3332, 3341, 3343, 3344, 3346, 3347, 3348, 3349, 3351, 3355, 3356, 3358, 3360, 3361, 3366, 3369, 3372, 3375, 3377, 3378, 3380, 3381, 3382, 3383, 3386, 3387, 3389, 3390, 3391, 3393, 3394, 3396, 3400, 3401, 3402, 3403, 3404, 3406, 3410, 3411, 3414, 3415, 3418, 3419, 3420, 3421, 3425, 3426, 3428, 3433, 3434, 3436, 3438, 3439, 3440, 3443, 3446, 3447, 3448, 3449, 3458, 3463, 3464, 3465, 3469, 3470, 3471, 3474, 3475, 3479, 3480, 3482, 3485, 3486, 3487, 3489, 3491, 3492, 3494, 3495, 3502, 3503, 3505, 3507, 3508, 3510, 3511, 3512, 3514, 3515, 3518, 3523, 3527, 3528, 3530, 3531, 3532, 3534, 3535, 3536, 3538, 3541, 3542, 3543, 3544, 3545, 3548, 3549, 3551, 3553, 3555, 3556, 3557, 3562, 3563, 3564, 3566, 3567, 3568, 3570, 3573, 3574, 3575, 3577, 3579, 3580, 3584, 3589, 3596, 3599, 3603, 3604, 3605, 3607, 3611, 3613, 3616, 3617, 3619, 3620, 3622, 3623, 3625, 3626, 3627, 3628, 3632, 3633, 3634, 3636, 3639, 3642, 3643, 3645, 3649, 3660, 3663, 3664, 3671, 3675, 3681, 3682, 3683, 3684, 3685, 3688, 3689, 3692, 3698, 3700, 3701, 3703, 3704, 3705, 3706, 3707, 3708, 3709, 3710, 3712, 3713, 3714, 3715, 3717, 3719, 3720, 3721, 3723, 3724, 3725, 3726, 3731, 3732, 3733, 3739, 3740, 3741, 3742, 3743, 3744, 3745, 3746, 3750, 3751, 3752, 3753, 3754, 3757, 3758, 3760, 3762, 3763, 3764, 3767, 3768, 3769, 3770, 3772, 3775, 3777, 3778, 3779, 3780, 3781, 3782, 3784, 3785, 3786, 3793, 3794, 3795, 3800, 3801, 3802, 3805, 3807, 3814, 3815, 3818, 3820, 3821, 3826, 3831, 3832, 3835, 3841, 3842, 3843, 3844, 3845, 3846, 3847, 3850, 3852, 3854, 3858, 3859, 3861, 3864, 3867, 3868, 3875, 3876, 3878, 3879, 3880, 3881, 3882, 3883, 3884, 3885, 3886, 3887, 3889, 3890, 3892, 3895, 3896, 3897, 3900, 3901, 3902, 3903, 3904, 3911, 3913, 3915, 3917, 3919, 3920, 3921, 3922, 3924, 3926, 3927, 3929, 3930, 3934, 3936, 3939, 3940, 3941, 3942, 3943, 3946, 3948, 3949, 3950, 3952, 3953, 3954, 3955, 3956, 3957, 3958, 3960, 3962, 3964, 3965, 3967, 3968, 3969, 3971, 3972, 3976, 3977, 3979, 3980, 3981, 3982, 3983, 3984, 3985, 3987, 3989, 3990, 3991, 3993, 3998, 4001, 4002, 4004, 4005, 4006, 4016, 4018, 4019, 4022, 4024, 4028, 4029, 4034, 4039, 4046, 4047, 4049, 4054, 4059, 4060, 4062, 4064, 4067, 4069, 4070, 4072, 4074, 4076, 4080, 4082, 4085, 4086, 4087, 4095, 4097, 4098, 4099, 4100, 4101, 4102, 4106, 4107, 4109, 4111, 4113, 4115, 4117, 4118, 4119, 4120, 4121, 4123, 4124, 4125, 4126, 4127, 4128, 4129, 4130, 4132, 4133, 4135, 4137, 4142, 4148, 4151, 4152, 4158, 4161, 4162, 4164, 4170, 4171, 4172, 4173, 4174, 4176, 4178, 4181, 4182, 4184, 4186, 4190, 4191, 4192, 4195, 4199, 4200, 4201, 4206, 4207, 4208, 4210, 4211, 4212, 4213, 4214, 4216, 4220, 4221, 4222, 4224, 4226, 4227, 4228, 4229, 4230, 4231, 4234, 4239, 4241, 4244, 4248, 4249, 4251, 4253, 4254, 4256, 4258, 4260, 4261, 4264, 4267, 4268, 4269, 4275, 4276, 4277, 4279, 4280, 4282, 4284, 4285, 4287, 4288, 4289, 4292, 4294, 4303, 4304, 4305, 4306, 4307, 4312, 4313, 4315, 4317, 4320, 4324, 4326, 4327, 4331, 4332, 4333, 4334, 4339, 4340, 4341, 4343, 4345, 4346, 4348, 4349, 4350, 4351, 4352, 4354, 4357, 4359, 4360, 4364, 4365, 4369, 4370, 4372, 4373, 4374, 4378, 4379, 4388, 4390, 4391, 4393, 4397, 4399, 4402, 4403, 4404, 4406, 4409, 4414, 4417, 4421, 4423, 4424, 4426, 4428, 4430, 4431, 4433, 4436, 4439, 4441, 4442, 4446, 4449, 4450, 4451, 4452, 4455, 4461, 4462, 4464, 4465, 4467, 4471, 4473, 4475, 4479, 4481, 4486, 4487, 4489, 4490, 4497, 4498, 4500, 4503, 4504, 4505, 4508, 4510, 4516, 4517, 4518, 4519, 4520, 4522, 4525, 4528, 4529, 4530, 4533, 4534, 4536, 4537, 4538, 4542, 4544, 4546, 4550, 4556, 4558, 4559, 4560, 4561, 4562, 4563, 4564, 4565, 4568, 4569, 4570, 4574, 4575, 4577, 4578, 4579, 4591, 4592, 4593, 4596, 4597, 4599, 4600, 4601, 4602, 4605, 4606, 4609, 4610, 4611, 4614, 4616, 4617, 4618, 4619, 4621, 4630, 4631, 4633, 4634, 4635, 4638, 4641, 4643, 4646, 4648, 4652, 4655, 4657, 4658, 4659, 4660, 4661, 4662, 4664, 4667, 4670, 4671, 4674, 4675, 4676, 4677, 4682, 4683, 4684, 4685, 4690, 4691, 4692, 4695, 4696, 4700, 4701, 4702, 4703, 4704, 4707, 4709, 4711, 4714, 4715, 4716, 4717, 4718, 4719, 4720, 4723, 4724, 4728, 4729, 4731, 4733, 4737, 4738, 4741, 4744, 4752, 4754, 4757, 4760, 4766, 4767, 4768, 4772, 4775, 4776, 4778, 4779, 4781, 4782, 4784, 4785, 4787, 4790, 4791, 4792, 4793, 4794, 4795, 4798, 4800, 4803, 4804, 4806, 4808, 4810, 4811, 4812, 4813, 4814, 4816, 4817, 4818, 4819, 4820, 4822, 4823, 4825, 4826, 4828, 4829, 4830, 4832, 4835, 4838, 4840, 4842, 4844, 4846, 4848, 4849, 4850, 4852, 4853, 4856, 4857, 4858, 4861, 4862, 4863, 4872, 4875, 4876, 4877, 4879, 4880, 4883, 4884, 4887, 4888, 4889, 4890, 4893, 4894, 4900, 4903, 4908, 4909, 4910, 4912, 4914, 4915, 4916, 4917, 4918, 4922, 4923, 4926, 4927, 4929, 4931, 4932, 4934, 4937, 4940, 4942, 4943, 4944, 4946, 4949, 4951, 4954, 4957, 4959, 4960, 4964, 4965, 4966, 4967, 4968, 4969, 4972, 4982, 4987, 4990, 4991, 4992, 4993, 4994, 4996, 4997, 4998, 4999, 5000, 5005, 5007, 5009, 5011, 5012, 5013, 5014, 5016, 5017, 5018, 5022, 5024, 5028, 5032, 5033, 5035, 5036, 5037, 5041, 5044, 5046, 5047, 5048, 5049, 5050, 5054, 5057, 5058, 5061, 5062, 5063, 5064, 5071, 5073, 5074, 5075, 5078, 5080, 5081, 5082, 5084, 5087, 5088, 5090, 5092, 5093, 5094, 5095, 5097, 5098, 5099, 5104, 5109, 5110, 5111, 5112, 5118, 5119, 5121, 5123, 5125, 5126, 5127, 5128, 5132, 5133, 5135, 5136, 5137, 5140, 5141, 5142, 5143, 5144, 5145, 5148, 5150, 5152, 5153, 5157, 5161, 5163, 5165, 5167, 5168, 5170, 5172, 5174, 5176, 5177, 5178, 5179, 5181, 5185, 5186, 5187, 5188, 5189, 5190, 5192, 5194, 5195, 5199, 5200, 5201, 5202, 5205, 5207, 5208, 5209, 5210, 5211, 5213, 5214, 5217, 5219, 5221, 5222, 5223, 5227, 5228, 5229, 5230, 5231, 5232, 5233, 5235, 5237, 5238, 5241, 5242, 5243, 5244, 5246, 5248, 5250, 5256, 5257, 5259, 5263, 5264, 5265, 5266, 5267, 5271, 5272, 5273, 5274, 5279, 5280, 5281, 5282, 5288, 5290, 5291, 5292, 5293, 5295, 5301, 5304, 5306, 5308, 5309, 5310, 5311, 5316, 5318, 5322, 5323, 5324, 5326, 5331, 5333, 5337, 5338, 5339, 5340, 5344, 5348, 5352, 5353, 5357, 5358, 5359, 5361, 5363, 5364, 5366, 5368, 5369, 5372, 5374, 5378, 5379, 5382, 5385, 5386, 5387, 5388, 5390, 5391, 5392, 5393, 5395, 5397, 5398, 5400, 5402, 5404, 5405, 5407, 5408, 5409, 5416, 5417, 5418, 5419, 5420, 5421, 5422, 5423, 5425, 5427, 5430, 5431, 5432, 5433, 5436, 5437, 5439, 5440, 5442, 5444, 5449, 5450, 5451, 5452, 5453, 5455, 5461, 5462, 5465, 5467, 5471, 5473, 5476, 5478, 5480, 5481, 5483, 5484, 5485, 5487, 5488, 5495, 5498, 5499, 5500, 5503, 5506, 5507, 5509, 5512, 5515, 5517, 5518, 5519, 5521, 5526, 5527, 5529, 5532, 5534, 5539, 5540, 5543, 5546, 5548, 5549, 5550, 5551, 5557, 5559, 5561, 5562, 5563, 5567, 5571, 5572, 5576, 5577, 5582, 5584, 5586, 5593, 5598, 5599, 5600, 5602, 5604, 5605, 5606, 5607, 5612, 5614, 5616, 5618, 5619, 5621, 5622, 5624, 5627, 5629, 5630, 5631, 5632, 5633, 5634, 5635, 5636, 5640, 5643, 5646, 5650, 5653, 5656, 5657, 5658, 5664, 5665, 5667, 5670, 5674, 5676, 5677, 5680, 5681, 5682, 5683, 5684, 5685, 5686, 5690, 5691, 5692, 5693, 5694, 5695, 5698, 5699, 5701, 5702, 5703, 5704, 5705, 5708, 5709, 5710, 5711, 5713, 5714, 5717, 5719, 5721, 5726, 5727, 5728, 5730, 5731, 5732, 5733, 5734, 5735, 5737, 5745, 5752, 5753, 5755, 5756, 5757, 5759, 5760, 5761, 5765, 5766, 5769, 5770, 5771, 5772, 5773, 5774, 5775, 5777, 5778, 5779, 5780, 5781, 5782, 5786, 5787, 5790, 5791, 5792, 5794, 5795, 5797, 5800, 5804, 5807, 5808, 5813, 5814, 5815, 5820, 5821, 5824, 5826, 5827, 5828, 5835, 5836, 5839, 5841, 5842, 5843, 5845, 5846, 5848, 5849, 5850, 5854, 5855, 5860, 5861, 5863, 5865, 5867, 5868, 5869, 5871, 5875, 5876, 5877, 5884, 5885, 5886, 5888, 5890, 5891, 5892, 5893, 5895, 5896, 5900, 5903, 5907, 5910, 5914, 5918, 5919, 5920, 5921, 5922, 5924, 5925, 5926, 5927, 5928, 5929, 5931, 5932, 5934, 5935, 5936, 5937, 5939, 5942, 5943, 5944, 5948, 5951, 5952, 5955, 5959, 5960, 5961, 5963, 5964, 5968, 5969, 5970, 5971, 5977, 5978, 5979, 5980, 5984, 5986, 5991, 5992, 5994, 5999, 6002, 6003, 6005, 6012, 6013, 6014, 6021, 6022, 6023, 6024, 6027, 6028, 6029, 6032, 6033, 6034, 6038, 6039, 6041, 6043, 6045])

#symbols = symbols[0:5]   #choose how many stocks you want to get data for. useful for creating smaller batches


ib_client = IBClient('127.0.0.1', 7497, 5, symbols, insert_to_db=write_to_db)

logging.info("Populating database for %s stocks. Symbols: %s", len(symbols), symbols)
logging.info("Started")

try:
    ib_client.fetch_historical_data(end_date_str, time_period, bar_size)

    while not all(ib_client.completion_status.values()):
        time.sleep(1)
        # Check for timeout
        if time.monotonic() - ib_client.last_update > timeout_duration:
            logging.error("Timeout occurred while fetching data.")
            missing_stocks = [stock_id for stock_id, completed in ib_client.completion_status.items() if not completed]
            logging.info("Missing stocks after timeout: %s", missing_stocks)
            #ib_client.mark_stocks_for_blacklisting(missing_stocks)
            break
        continue

    blacklist_stocks(ib_client.symbols_to_blacklist, symbols, write_to_db=write_to_db)
    logging.info("Fetched data but failed to insert into database for %s stocks: %s", len(ib_client.failed_to_insert_symbols), ib_client.failed_to_insert_symbols)
    logging.info("Received errors for %s stocks. Review for retry or blacklist: %s", len(ib_client.erroneous_symbols), [error[0] for error in ib_client.erroneous_symbols])
    logging.info("Details of erroneous stocks: %s", ib_client.erroneous_symbols)

except Exception as e:
    logging.error("Error fetching data: %s", e)

finally:
    logging.info("Finished")
    ib_client.disconnect()


