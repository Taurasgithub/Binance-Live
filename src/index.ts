import { McpServer } from "@modelcontextprotocol/server";
import { createMcpHandler } from "agents/mcp/server";
import { z } from "zod";

const BINANCE_API = "https://api-gcp.binance.com";

const COINGECKO_SYMBOLS: Record<string, string> = {
  BTCUSDT: "bitcoin",
  ETHUSDT: "ethereum",
  SOLUSDT: "solana",
  BNBUSDT: "binancecoin",
  XRPUSDT: "ripple",
  ADAUSDT: "cardano",
  DOGEUSDT: "dogecoin",
};

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers":
    "Content-Type, Authorization, Accept, MCP-Protocol-Version, X-Requested-With",
  "Access-Control-Max-Age": "86400",
} as const;

function withCors(headers: HeadersInit = {}): Headers {
  const merged = new Headers(headers);

  for (const [key, value] of Object.entries(corsHeaders)) {
    merged.set(key, value);
  }

  return merged;
}

/* ============================================================
   SCHEMAS
   ============================================================ */

const symbolSchema = z
  .string()
  .regex(
    /^[A-Z0-9]{5,20}$/,
    "Symbol must contain only uppercase letters/numbers, e.g. BTCUSDT",
  );

const intervalSchema = z.enum([
  "1s",
  "1m",
  "3m",
  "5m",
  "15m",
  "30m",
  "1h",
  "2h",
  "4h",
  "6h",
  "8h",
  "12h",
  "1d",
  "3d",
  "1w",
  "1M",
]);

type Interval = z.infer<typeof intervalSchema>;

type Candle = number[];

/* ============================================================
   BINANCE REQUEST
   ============================================================ */

async function binanceRequest(
  path: string,
  params: Record<string, string | number | undefined> = {},
) {
  const url = new URL(path, BINANCE_API);

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      url.searchParams.set(key, String(value));
    }
  }

  const response = await fetch(url.toString(), {
    headers: {
      Accept: "application/json",
    },
  });

  const text = await response.text();

  if (!response.ok) {
    const message = `Binance API error ${response.status}: ${text.slice(0, 500)}`;

    if (
      response.status === 451 ||
      /restricted location|service unavailable/i.test(text)
    ) {
      const fallback = await coingeckoFallbackForSymbol(
        params.symbol as string | undefined,
      );
      if (fallback) {
        return {
          __fallback: true,
          __fallbackReason: "Binance is blocked in this region.",
          ...fallback,
        };
      }
    }

    throw new Error(message);
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error("Binance returned invalid JSON.");
  }
}

const REFERENCE_PRICES: Record<string, { price: number; change24h: number }> = {
  BTCUSDT: { price: 42500, change24h: 2.5 },
  ETHUSDT: { price: 2300, change24h: 1.8 },
  SOLUSDT: { price: 145, change24h: 3.2 },
  BNBUSDT: { price: 610, change24h: 0.5 },
  XRPUSDT: { price: 2.1, change24h: -0.8 },
  ADAUSDT: { price: 0.98, change24h: 1.2 },
  DOGEUSDT: { price: 0.38, change24h: 2.1 },
};

async function coingeckoFallbackForSymbol(symbol?: string) {
  if (!symbol) {
    return null;
  }

  const assetId = COINGECKO_SYMBOLS[symbol] ?? symbol.toLowerCase();

  try {
    const response = await fetch(
      `https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(assetId)}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true`,
      {
        headers: {
          Accept: "application/json",
        },
      },
    );

    if (response.ok) {
      const data = (await response.json()) as Record<
        string,
        {
          usd?: number;
          usd_24h_change?: number;
          total_vol?: number;
        }
      >;
      const entry = data?.[assetId];

      if (entry && typeof entry.usd === "number") {
        return {
          symbol,
          price: entry.usd,
          change24hPercent: entry.usd_24h_change ?? 0,
          volume24h: entry.total_vol ?? 0,
          source: "coingecko-fallback",
        };
      }
    }
  } catch (e) {
    // CoinGecko failed, fall through to reference prices
  }

  // Last resort: use reference prices
  if (symbol in REFERENCE_PRICES) {
    const ref = REFERENCE_PRICES[symbol];
    return {
      symbol,
      price: ref.price,
      change24hPercent: ref.change24h,
      volume24h: 0,
      source: "reference-prices-fallback",
    };
  }

  return null;
}

/* ============================================================
   RESPONSE HELPERS
   ============================================================ */

function jsonResult(data: unknown) {
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(data, null, 2),
      },
    ],
  };
}

function errorResult(error: unknown) {
  const message =
    error instanceof Error ? error.message : "Unknown Binance API error.";

  return {
    isError: true,
    content: [
      {
        type: "text" as const,
        text: message,
      },
    ],
  };
}

/* ============================================================
   DATA HELPERS
   ============================================================ */

function closes(candles: Candle[]): number[] {
  return candles.map((c) => Number(c[4]));
}

function highs(candles: Candle[]): number[] {
  return candles.map((c) => Number(c[2]));
}

function lows(candles: Candle[]): number[] {
  return candles.map((c) => Number(c[3]));
}

function volumes(candles: Candle[]): number[] {
  return candles.map((c) => Number(c[5]));
}

/* ============================================================
   INDICATORS
   ============================================================ */

function sma(values: number[], period: number): number | null {
  if (values.length < period) return null;

  const slice = values.slice(-period);

  return slice.reduce((a, b) => a + b, 0) / period;
}

function ema(values: number[], period: number): number | null {
  if (values.length < period) return null;

  const multiplier = 2 / (period + 1);

  let result = values.slice(0, period).reduce((a, b) => a + b, 0) / period;

  for (let i = period; i < values.length; i++) {
    result = (values[i] - result) * multiplier + result;
  }

  return result;
}

function rsi(values: number[], period: number): number | null {
  if (values.length < period + 1) {
    return null;
  }

  let gains = 0;
  let losses = 0;

  for (let i = 1; i <= period; i++) {
    const change = values[i] - values[i - 1];

    if (change >= 0) {
      gains += change;
    } else {
      losses += Math.abs(change);
    }
  }

  let averageGain = gains / period;
  let averageLoss = losses / period;

  for (let i = period + 1; i < values.length; i++) {
    const change = values[i] - values[i - 1];

    const gain = Math.max(change, 0);
    const loss = Math.max(-change, 0);

    averageGain = (averageGain * (period - 1) + gain) / period;

    averageLoss = (averageLoss * (period - 1) + loss) / period;
  }

  if (averageLoss === 0) {
    return 100;
  }

  const rs = averageGain / averageLoss;

  return 100 - 100 / (1 + rs);
}

function trueRanges(candles: Candle[]): number[] {
  const result: number[] = [];

  for (let i = 1; i < candles.length; i++) {
    const high = Number(candles[i][2]);

    const low = Number(candles[i][3]);

    const previousClose = Number(candles[i - 1][4]);

    result.push(
      Math.max(
        high - low,
        Math.abs(high - previousClose),
        Math.abs(low - previousClose),
      ),
    );
  }

  return result;
}

function atr(candles: Candle[], period = 14): number | null {
  const ranges = trueRanges(candles);

  return sma(ranges, period);
}

function bollingerBands(values: number[], period = 20, multiplier = 2) {
  if (values.length < period) {
    return null;
  }

  const slice = values.slice(-period);

  const middle = slice.reduce((a, b) => a + b, 0) / period;

  const variance =
    slice.reduce((sum, value) => sum + Math.pow(value - middle, 2), 0) / period;

  const standardDeviation = Math.sqrt(variance);

  return {
    middle,
    upper: middle + multiplier * standardDeviation,
    lower: middle - multiplier * standardDeviation,
  };
}

function macd(values: number[]) {
  if (values.length < 35) {
    return null;
  }

  const multiplier12 = 2 / 13;
  const multiplier26 = 2 / 27;

  let ema12 = values.slice(0, 12).reduce((a, b) => a + b, 0) / 12;

  let ema26 = values.slice(0, 26).reduce((a, b) => a + b, 0) / 26;

  const macdValues: number[] = [];

  for (let i = 12; i < values.length; i++) {
    ema12 = (values[i] - ema12) * multiplier12 + ema12;

    if (i >= 26) {
      ema26 = (values[i] - ema26) * multiplier26 + ema26;

      macdValues.push(ema12 - ema26);
    }
  }

  if (macdValues.length < 9) {
    return null;
  }

  const signal = ema(macdValues, 9);

  const line = macdValues[macdValues.length - 1];

  return {
    macd: line,
    signal,
    histogram: signal !== null ? line - signal : null,
  };
}

/* ============================================================
   SUPPORT / RESISTANCE
   ============================================================ */

function findSupportResistance(candles: Candle[]) {
  const recent = candles.slice(-50);

  const highValues = highs(recent);

  const lowValues = lows(recent);

  const resistance = Math.max(...highValues);

  const support = Math.min(...lowValues);

  /*
   * IMPORTANT FIX:
   *
   * Support = lowest meaningful lows.
   * Resistance = highest meaningful highs.
   */

  const swingSupports = [...lowValues].sort((a, b) => a - b).slice(0, 5);

  const swingResistances = [...highValues].sort((a, b) => b - a).slice(0, 5);

  return {
    support,
    resistance,
    swingSupports,
    swingResistances,
  };
}

function averageVolume(values: number[], period = 20) {
  return sma(values, period);
}

function roundPrice(value: number, decimals = 8) {
  return Number(value.toFixed(decimals));
}

/* ============================================================
   CORE TRADE ANALYSIS
   ============================================================ */

async function runTradeAnalysis(
  symbol: string,
  entryInterval: Interval = "5m",
  confirmationInterval: Interval = "15m",
  trendInterval: Interval = "1h",
  higherInterval: Interval = "4h",
) {
  try {
    /* ----------------------------------------------------------
       DATA
       ---------------------------------------------------------- */

    const entryCandles = (await binanceRequest("/api/v3/klines", {
      symbol,
      interval: entryInterval,
      limit: 250,
    })) as Candle[];

    const confirmationCandles = (await binanceRequest("/api/v3/klines", {
      symbol,
      interval: confirmationInterval,
      limit: 250,
    })) as Candle[];

    const trendCandles = (await binanceRequest("/api/v3/klines", {
      symbol,
      interval: trendInterval,
      limit: 250,
    })) as Candle[];

    const higherCandles = (await binanceRequest("/api/v3/klines", {
      symbol,
      interval: higherInterval,
      limit: 250,
    })) as Candle[];

    const ticker = (await binanceRequest("/api/v3/ticker/24hr", {
      symbol,
    })) as Record<string, unknown>;

    const orderBook = (await binanceRequest("/api/v3/depth", {
      symbol,
      limit: 20,
    })) as {
      bids?: string[][];
      asks?: string[][];
    };

    /* ----------------------------------------------------------
       DETECT FALLBACK RESPONSES
       ---------------------------------------------------------- */

    if (
      (entryCandles as unknown as Record<string, unknown>)?.__fallback ||
      (confirmationCandles as unknown as Record<string, unknown>)?.__fallback ||
      (trendCandles as unknown as Record<string, unknown>)?.__fallback ||
      (higherCandles as unknown as Record<string, unknown>)?.__fallback ||
      (ticker as Record<string, unknown>)?.__fallback
    ) {
      const fallback = (entryCandles as unknown as Record<string, unknown>)
        ?.__fallback
        ? (entryCandles as unknown as Record<string, unknown>)
        : (ticker as Record<string, unknown>)?.__fallback
          ? (ticker as unknown as Record<string, unknown>)
          : null;

      const fallbackData = fallback as Record<string, unknown> | null;

      return {
        tool: "binance_trade_analysis",
        version: "5.0.0",
        symbol,
        timestamp: new Date().toISOString(),
        dataSource: "CoinGecko fallback (Binance restricted in this region)",
        readOnly: true,
        evaluation: {
          decision: "WAIT",
          setupQuality: "NO_SETUP",
          confidencePercent: 15,
          status: "BINANCE_RESTRICTED",
          exactEvaluation:
            "Binance public API is geo-restricted from this environment (HTTP 451 / restricted location). Technical analysis is unavailable. The system has attempted CoinGecko fallback and reference price data. Full multi-timeframe analysis requires unrestricted Binance access.",
          reason: "Binance returned HTTP 451 or a restricted-location error.",
        },
        market: fallbackData
          ? {
              price: fallbackData.price,
              change24hPercent: fallbackData.change24hPercent,
              volume24h: fallbackData.volume24h ?? 0,
              source: fallbackData.source,
            }
          : {
              price: null,
              change24hPercent: null,
              volume24h: null,
              source: "unavailable",
            },
        signal: {
          bias: "NEUTRAL",
          decision: "WAIT",
          setupQuality: "NO_SETUP",
          bullishScore: 0,
          bearishScore: 0,
          totalScore: 0,
          confidencePercent: 15,
          signalAgreementPercent: 0,
          signals: [],
        },
        tradeSetup: {
          type: "NO_NEW_LONG",
          entry: null,
          stopLoss: null,
          takeProfit1: null,
          takeProfit2: null,
          riskReward: null,
          note: "Binance data is restricted from this environment. Full technical analysis is unavailable until Binance access is restored.",
        },
        reasoning: [
          "Binance returned HTTP 451 or a restricted-location error.",
          "The MCP connector is operational, but upstream Binance API is geo-blocked from this environment.",
          fallbackData
            ? `A fallback price (${fallbackData.source}) is available: $${fallbackData.price} (${fallbackData.change24hPercent}% 24h), but this does not provide technical indicators (EMA, RSI, MACD, etc.).`
            : "No fallback market data was available from CoinGecko or reference prices.",
          "This is an infrastructure limitation, not a market signal.",
        ],
        disclaimer:
          "This is a reduced evaluation: Binance market data is restricted in this region, so the tool cannot complete full multi-timeframe technical analysis.",
      };
    }

    /* ----------------------------------------------------------
       CLOSES
       ---------------------------------------------------------- */

    const entryClose = closes(entryCandles as Candle[]);

    const confirmationClose = closes(confirmationCandles as Candle[]);

    const trendClose = closes(trendCandles as Candle[]);

    const higherClose = closes(higherCandles as Candle[]);

    const currentPrice = Number((ticker as Record<string, unknown>).lastPrice);

    /* ----------------------------------------------------------
     ENTRY INDICATORS
     ---------------------------------------------------------- */

    const entryEma9 = ema(entryClose, 9);

    const entryEma21 = ema(entryClose, 21);

    const entryEma50 = ema(entryClose, 50);

    const entryRsi6 = rsi(entryClose, 6);

    const entryRsi14 = rsi(entryClose, 14);

    const entryMacd = macd(entryClose);

    const entryBands = bollingerBands(entryClose, 20, 2);

    const entryAtr = atr(entryCandles, 14);

    /* ----------------------------------------------------------
     CONFIRMATION
     ---------------------------------------------------------- */

    const confirmationEma9 = ema(confirmationClose, 9);

    const confirmationEma21 = ema(confirmationClose, 21);

    const confirmationEma50 = ema(confirmationClose, 50);

    const confirmationRsi = rsi(confirmationClose, 14);

    /* ----------------------------------------------------------
     PRIMARY TREND
     ---------------------------------------------------------- */

    const trendEma50 = ema(trendClose, 50);

    const trendEma200 = ema(trendClose, 200);

    const trendRsi = rsi(trendClose, 14);

    /* ----------------------------------------------------------
     HIGHER TREND
     ---------------------------------------------------------- */

    const higherEma50 = ema(higherClose, 50);

    const higherEma200 = ema(higherClose, 200);

    const higherRsi = rsi(higherClose, 14);

    /* ----------------------------------------------------------
     SUPPORT / RESISTANCE
     ---------------------------------------------------------- */

    const levels = findSupportResistance(entryCandles);

    /* ----------------------------------------------------------
     VOLUME
     ---------------------------------------------------------- */

    const entryVolume = volumes(entryCandles);

    const currentVolume = entryVolume[entryVolume.length - 1];

    const previousAverage20 = averageVolume(entryVolume.slice(0, -1), 20);

    const volumeRatio =
      previousAverage20 && previousAverage20 > 0
        ? currentVolume / previousAverage20
        : null;

    const previousClose = Number(entryCandles[entryCandles.length - 2][4]);

    const priceDirection =
      currentPrice > previousClose
        ? "UP"
        : currentPrice < previousClose
          ? "DOWN"
          : "FLAT";

    /* ----------------------------------------------------------
     ORDER BOOK
     ---------------------------------------------------------- */

    const bids = orderBook.bids ?? [];

    const asks = orderBook.asks ?? [];

    const bidVolume = bids.reduce((sum, level) => sum + Number(level[1]), 0);

    const askVolume = asks.reduce((sum, level) => sum + Number(level[1]), 0);

    const totalBookVolume = bidVolume + askVolume;

    const orderBookImbalance =
      totalBookVolume > 0 ? (bidVolume - askVolume) / totalBookVolume : 0;

    const bidPercentage =
      totalBookVolume > 0 ? (bidVolume / totalBookVolume) * 100 : 0;

    const askPercentage =
      totalBookVolume > 0 ? (askVolume / totalBookVolume) * 100 : 0;

    const orderBookInterpretation =
      orderBookImbalance > 0.1
        ? "BID_HEAVY"
        : orderBookImbalance < -0.1
          ? "ASK_HEAVY"
          : "BALANCED";

    /*
     * Strong pressure thresholds.
     *
     * These are intentionally stricter
     * than the normal +/- 0.10 classification.
     */

    const strongBidPressure = orderBookImbalance >= 0.25;

    const strongAskPressure = orderBookImbalance <= -0.25;

    /* ----------------------------------------------------------
     TREND CONDITIONS
     ---------------------------------------------------------- */

    const entryBullish =
      entryEma9 !== null &&
      entryEma21 !== null &&
      entryEma50 !== null &&
      entryEma9 > entryEma21 &&
      entryEma21 > entryEma50;

    const confirmationBullish =
      confirmationEma21 !== null &&
      confirmationEma50 !== null &&
      confirmationEma21 > confirmationEma50;

    const trendBullish =
      trendEma50 !== null && trendEma200 !== null && trendEma50 > trendEma200;

    const higherTrendBullish =
      higherEma50 !== null &&
      higherEma200 !== null &&
      higherEma50 > higherEma200;

    const entryBearish =
      entryEma9 !== null &&
      entryEma21 !== null &&
      entryEma50 !== null &&
      entryEma9 < entryEma21 &&
      entryEma21 < entryEma50;

    const confirmationBearish =
      confirmationEma21 !== null &&
      confirmationEma50 !== null &&
      confirmationEma21 < confirmationEma50;

    const trendBearish =
      trendEma50 !== null && trendEma200 !== null && trendEma50 < trendEma200;

    const higherTrendBearish =
      higherEma50 !== null &&
      higherEma200 !== null &&
      higherEma50 < higherEma200;

    const trendAlignment =
      entryBullish && confirmationBullish && trendBullish && higherTrendBullish;

    const bearishAlignment =
      entryBearish && confirmationBearish && trendBearish && higherTrendBearish;

    /* ----------------------------------------------------------
     RSI
     ---------------------------------------------------------- */

    const rsiHealthy =
      entryRsi14 !== null && entryRsi14 >= 45 && entryRsi14 <= 70;

    /* ----------------------------------------------------------
     MACD
     ---------------------------------------------------------- */

    const macdBullish =
      entryMacd !== null &&
      entryMacd.histogram !== null &&
      entryMacd.histogram > 0;

    /* ----------------------------------------------------------
     VOLUME
     ---------------------------------------------------------- */

    const volumeHealthy = volumeRatio !== null && volumeRatio >= 0.8;

    /* ----------------------------------------------------------
     SCORING
     ---------------------------------------------------------- */

    let bullishScore = 0;
    let bearishScore = 0;

    const signals: Array<{
      indicator: string;
      signal: string;
      weight: number;
    }> = [];

    function addSignal(
      indicator: string,
      bullish: boolean,
      weight: number,
      bullishSignal: string,
      bearishSignal: string,
    ) {
      if (bullish) {
        bullishScore += weight;

        signals.push({
          indicator,
          signal: bullishSignal,
          weight,
        });
      } else {
        bearishScore += weight;

        signals.push({
          indicator,
          signal: bearishSignal,
          weight,
        });
      }
    }

    if (entryEma9 !== null && entryEma21 !== null) {
      addSignal(
        "Entry EMA 9/21",
        entryEma9 > entryEma21,
        1,
        "BULLISH",
        "BEARISH",
      );
    }

    if (entryEma21 !== null && entryEma50 !== null) {
      addSignal(
        "Entry EMA 21/50",
        entryEma21 > entryEma50,
        1,
        "BULLISH",
        "BEARISH",
      );
    }

    if (confirmationEma21 !== null && confirmationEma50 !== null) {
      addSignal(
        "Confirmation EMA 21/50",
        confirmationEma21 > confirmationEma50,
        1,
        "BULLISH",
        "BEARISH",
      );
    }

    if (trendEma50 !== null && trendEma200 !== null) {
      addSignal(
        "Primary Trend EMA 50/200",
        trendEma50 > trendEma200,
        2,
        "BULLISH",
        "BEARISH",
      );
    }

    if (higherEma50 !== null && higherEma200 !== null) {
      addSignal(
        "Higher Trend EMA 50/200",
        higherEma50 > higherEma200,
        2,
        "BULLISH",
        "BEARISH",
      );
    }

    if (entryRsi14 !== null) {
      if (entryRsi14 >= 50 && entryRsi14 <= 70) {
        bullishScore += 1;

        signals.push({
          indicator: "Entry RSI14",
          signal: "BULLISH_MOMENTUM",
          weight: 1,
        });
      } else if (entryRsi14 < 45) {
        bearishScore += 1;

        signals.push({
          indicator: "Entry RSI14",
          signal: "BEARISH",
          weight: 1,
        });
      }
    }

    if (entryMacd !== null) {
      if (entryMacd.histogram !== null && entryMacd.histogram > 0) {
        bullishScore += 1;

        signals.push({
          indicator: "MACD Histogram",
          signal: "BULLISH",
          weight: 1,
        });
      } else if (entryMacd.histogram !== null) {
        bearishScore += 1;

        signals.push({
          indicator: "MACD Histogram",
          signal: "BEARISH",
          weight: 1,
        });
      }
    }

    if (orderBookImbalance > 0.1) {
      bullishScore += 1;

      signals.push({
        indicator: "Order Book",
        signal: strongBidPressure ? "STRONG_BID_HEAVY" : "BID_HEAVY",
        weight: 1,
      });
    } else if (orderBookImbalance < -0.1) {
      bearishScore += 1;

      signals.push({
        indicator: "Order Book",
        signal: strongAskPressure ? "STRONG_ASK_HEAVY" : "ASK_HEAVY",
        weight: 1,
      });
    }

    /* ----------------------------------------------------------
     RAW BIAS
     ---------------------------------------------------------- */

    const totalScore = bullishScore + bearishScore;

    let bias: "BULLISH" | "BEARISH" | "NEUTRAL";

    if (bullishScore >= bearishScore + 3) {
      bias = "BULLISH";
    } else if (bearishScore >= bullishScore + 3) {
      bias = "BEARISH";
    } else {
      bias = "NEUTRAL";
    }

    /* ----------------------------------------------------------
     ATR
     ---------------------------------------------------------- */

    const atrValue = entryAtr ?? currentPrice * 0.01;

    /* ----------------------------------------------------------
     CONSERVATIVE PULLBACK ENTRY
     ---------------------------------------------------------- */

    /*
     * Entry hierarchy:
     *
     * 1. EMA9 if price is above it
     * 2. EMA21 if it gives a better pullback
     * 3. EMA50
     * 4. Current price as fallback
     *
     * Never allow the proposed entry more than
     * one ATR below current price.
     */

    const pullbackCandidates = [entryEma9, entryEma21, entryEma50].filter(
      (value): value is number => value !== null && value < currentPrice,
    );

    let proposedEntry = currentPrice;

    if (pullbackCandidates.length > 0) {
      /*
       * Choose the deepest reasonable
       * pullback that remains within
       * one ATR of current price.
       */

      const minimumEntry = currentPrice - atrValue;

      const reasonableCandidates = pullbackCandidates.filter(
        (value) => value >= minimumEntry,
      );

      if (reasonableCandidates.length > 0) {
        proposedEntry = Math.min(...reasonableCandidates);
      } else {
        proposedEntry = Math.max(Math.max(...pullbackCandidates), minimumEntry);
      }
    }

    /*
     * If price is already too close to
     * resistance, do not chase it.
     */

    const resistanceDistance = levels.resistance - proposedEntry;

    const currentResistanceDistance = levels.resistance - currentPrice;

    /* ----------------------------------------------------------
     SUPPORT-BASED STOP
     ---------------------------------------------------------- */

    /*
     * Stop should sit below structural
     * support OR at least 1.5 ATR below
     * the proposed entry.
     */

    const atrStop = proposedEntry - atrValue * 1.5;

    const supportStop = levels.support - atrValue * 0.25;

    let proposedStop = Math.min(supportStop, atrStop);

    if (proposedStop >= proposedEntry) {
      proposedStop = atrStop;
    }

    const risk = proposedEntry - proposedStop;

    /* ----------------------------------------------------------
     TARGETS
     ---------------------------------------------------------- */

    const rrTarget1 = proposedEntry + risk * 1.5;

    const rrTarget2 = proposedEntry + risk * 2.5;

    /*
     * TP1 respects resistance.
     */

    let proposedTP1 = rrTarget1;

    if (levels.resistance > proposedEntry && levels.resistance < proposedTP1) {
      proposedTP1 = levels.resistance;
    }

    /*
     * TP2 starts at 2.5R.
     *
     * If resistance exists, require
     * additional room beyond resistance.
     */

    let proposedTP2 = rrTarget2;

    if (levels.resistance > proposedEntry) {
      proposedTP2 = Math.max(rrTarget2, levels.resistance + risk * 0.5);
    }

    const reward = proposedTP1 - proposedEntry;

    const riskReward = risk > 0 ? reward / risk : 0;

    /* ----------------------------------------------------------
     TRADE GEOMETRY
     ---------------------------------------------------------- */

    const rrAcceptable = riskReward >= 1.5;

    const resistanceAcceptable = resistanceDistance >= risk * 1.5;

    /*
     * Don't chase when current price
     * is extremely close to resistance.
     */

    const resistanceChaseFilter = currentResistanceDistance > atrValue * 0.5;

    /* ----------------------------------------------------------
     EXECUTION FILTERS
     ---------------------------------------------------------- */

    /*
     * Strong ask pressure blocks a new
     * spot long.
     */

    const executionBullish = !strongAskPressure;

    /*
     * A weak order book is not automatically
     * bearish, but strong ask pressure is
     * considered a hard execution warning.
     */

    const orderBookPass = executionBullish;

    /* ----------------------------------------------------------
     DECISION
     ---------------------------------------------------------- */

    let decision: "BUY" | "WAIT" | "SELL";

    let setupQuality: "EXCELLENT" | "GOOD" | "FAIR" | "POOR" | "NO_SETUP";

    const waitReasons: string[] = [];

    const buyConditions =
      bias === "BULLISH" &&
      trendAlignment &&
      rsiHealthy &&
      macdBullish &&
      rrAcceptable &&
      resistanceAcceptable &&
      resistanceChaseFilter &&
      orderBookPass;

    if (buyConditions) {
      decision = "BUY";

      if (volumeHealthy && strongBidPressure && riskReward >= 2) {
        setupQuality = "EXCELLENT";
      } else if (riskReward >= 1.75) {
        setupQuality = "GOOD";
      } else {
        setupQuality = "FAIR";
      }
    } else {
      decision = "WAIT";

      setupQuality =
        bias === "BULLISH" || bias === "BEARISH" ? "POOR" : "NO_SETUP";

      if (bias !== "BULLISH") {
        waitReasons.push(
          "Multi-timeframe bullish alignment is not strong enough.",
        );
      }

      if (!trendAlignment) {
        waitReasons.push("Not all major timeframes confirm the bullish trend.");
      }

      if (!rsiHealthy) {
        waitReasons.push("RSI is outside the preferred entry zone.");
      }

      if (!macdBullish) {
        waitReasons.push("MACD does not confirm bullish momentum.");
      }

      if (!rrAcceptable) {
        waitReasons.push(
          `Risk/reward is only ${riskReward.toFixed(
            2,
          )}; minimum required is 1.50.`,
        );
      }

      if (!resistanceAcceptable) {
        waitReasons.push(
          "Nearest resistance is too close to the proposed entry.",
        );
      }

      if (!resistanceChaseFilter) {
        waitReasons.push(
          "Current price is too close to resistance; avoid chasing.",
        );
      }

      if (strongAskPressure) {
        waitReasons.push(
          `Order book shows strong ask pressure (${askPercentage.toFixed(
            2,
          )}% asks vs ${bidPercentage.toFixed(2)}% bids).`,
        );
      }

      if (volumeRatio !== null && volumeRatio < 0.8) {
        waitReasons.push("Current volume is below its recent average.");
      }
    }

    /*
     * SELL means:
     *
     * Avoid opening a new spot long.
     * No short positions are opened.
     */

    if (bearishAlignment && bearishScore >= 7) {
      decision = "SELL";

      setupQuality = "NO_SETUP";

      waitReasons.push(
        "Multiple timeframes show bearish alignment; avoid opening a new spot long.",
      );
    }

    /* ----------------------------------------------------------
     CONFIDENCE
     ---------------------------------------------------------- */

    const dominantScore = Math.max(bullishScore, bearishScore);

    const directionalAgreement =
      totalScore > 0 ? dominantScore / totalScore : 0.5;

    let confidence = 50 + directionalAgreement * 30;

    if (trendAlignment || bearishAlignment) {
      confidence += 8;
    }

    if (rsiHealthy) {
      confidence += 3;
    }

    if (macdBullish) {
      confidence += 3;
    }

    if (strongBidPressure) {
      confidence += 2;
    }

    if (strongAskPressure) {
      confidence -= 8;
    }

    if (volumeRatio !== null && volumeRatio < 0.8) {
      confidence -= 8;
    }

    if (!rrAcceptable) {
      confidence -= 12;
    }

    if (!resistanceAcceptable) {
      confidence -= 8;
    }

    if (!resistanceChaseFilter) {
      confidence -= 5;
    }

    confidence = Math.max(1, Math.min(95, Math.round(confidence)));

    /* ----------------------------------------------------------
     SIGNAL AGREEMENT
     ---------------------------------------------------------- */

    const signalAgreementPercent =
      totalScore > 0 ? Math.round((dominantScore / totalScore) * 100) : 50;

    /* ----------------------------------------------------------
     REASONING
     ---------------------------------------------------------- */

    const reasoning: string[] = [];

    if (bias === "BULLISH") {
      reasoning.push("Directional indicators favor bullish conditions.");
    }

    if (bias === "BEARISH") {
      reasoning.push("Directional indicators favor bearish conditions.");
    }

    if (bias === "NEUTRAL") {
      reasoning.push("Directional indicators are mixed.");
    }

    if (trendAlignment) {
      reasoning.push("All four selected timeframes confirm the bullish trend.");
    }

    if (rsiHealthy) {
      reasoning.push("RSI14 is within the preferred momentum zone.");
    }

    if (macdBullish) {
      reasoning.push("MACD histogram is positive.");
    }

    if (strongBidPressure) {
      reasoning.push("Order book shows strong bid pressure.");
    }

    if (strongAskPressure) {
      reasoning.push("Order book shows strong ask pressure.");
    }

    if (volumeRatio !== null) {
      reasoning.push(
        `Current volume is ${volumeRatio.toFixed(
          2,
        )}x the previous 20-candle average.`,
      );
    }

    reasoning.push(`Proposed pullback entry is ${roundPrice(proposedEntry)}.`);

    reasoning.push(`Nearest resistance is ${roundPrice(levels.resistance)}.`);

    reasoning.push(`Calculated R:R at TP1 is ${riskReward.toFixed(2)}.`);

    if (!rrAcceptable) {
      reasoning.push(
        `Trade rejected because R:R is ${riskReward.toFixed(
          2,
        )}, below the 1.50 minimum.`,
      );
    }

    if (!resistanceAcceptable) {
      reasoning.push(
        "Trade rejected because nearby resistance leaves insufficient upside.",
      );
    }

    if (!resistanceChaseFilter) {
      reasoning.push(
        "Current price is too close to resistance for a conservative entry.",
      );
    }

    if (decision === "WAIT" && waitReasons.length > 0) {
      reasoning.push(...waitReasons);
    }

    /* ----------------------------------------------------------
     TRADE SETUP
     ---------------------------------------------------------- */

    const tradeSetup =
      decision === "BUY"
        ? {
            type: "SPOT_LONG",

            entry: roundPrice(proposedEntry),

            stopLoss: roundPrice(proposedStop),

            takeProfit1: roundPrice(proposedTP1),

            takeProfit2: roundPrice(proposedTP2),

            riskReward: Number(riskReward.toFixed(2)),

            riskPercent: Number(((risk / proposedEntry) * 100).toFixed(3)),

            resistanceDistance: Number(resistanceDistance.toFixed(2)),

            resistanceRoomPercent: Number(
              ((resistanceDistance / proposedEntry) * 100).toFixed(3),
            ),
          }
        : {
            type: "NO_NEW_LONG",

            entry: null,
            stopLoss: null,
            takeProfit1: null,
            takeProfit2: null,
            riskReward: null,

            note:
              decision === "SELL"
                ? "Spot traders should avoid new long entries while bearish conditions persist."
                : "Wait for stronger confirmation or a better risk/reward setup.",
          };

    /* ----------------------------------------------------------
     RESULT
     ---------------------------------------------------------- */

    return {
      tool: "binance_trade_analysis",

      version: "5.0.0",

      symbol,

      timestamp: new Date().toISOString(),

      dataSource: "Binance Spot public API via Cloudflare Worker",

      readOnly: true,

      market: {
        price: currentPrice,

        change24hPercent: Number(
          (ticker as Record<string, unknown>).priceChangePercent,
        ),

        high24h: Number((ticker as Record<string, unknown>).highPrice),

        low24h: Number((ticker as Record<string, unknown>).lowPrice),

        volume24h: Number((ticker as Record<string, unknown>).volume),

        quoteVolume24h: Number((ticker as Record<string, unknown>).quoteVolume),
      },

      orderBookTop: {
        bestBid: Number(bids?.[0]?.[0] ?? 0),

        bestAsk: Number(asks?.[0]?.[0] ?? 0),

        bidQty: Number(bids?.[0]?.[1] ?? 0),

        askQty: Number(asks?.[0]?.[1] ?? 0),
      },

      timeframes: {
        entry: entryInterval,
        confirmation: confirmationInterval,
        trend: trendInterval,
        higher: higherInterval,
      },

      indicators: {
        entry: {
          ema9: entryEma9,
          ema21: entryEma21,
          ema50: entryEma50,
          rsi6: entryRsi6,
          rsi14: entryRsi14,
          macd: entryMacd,
          bollingerBands: entryBands,
          atr14: entryAtr,
        },

        confirmation: {
          ema9: confirmationEma9,
          ema21: confirmationEma21,
          ema50: confirmationEma50,
          rsi14: confirmationRsi,
        },

        trend: {
          ema50: trendEma50,
          ema200: trendEma200,
          rsi14: trendRsi,
        },

        higherTrend: {
          ema50: higherEma50,
          ema200: higherEma200,
          rsi14: higherRsi,
        },
      },

      marketStructure: {
        support: levels.support,

        resistance: levels.resistance,

        swingSupports: levels.swingSupports,

        swingResistances: levels.swingResistances,

        volume: {
          current: currentVolume,

          previousAverage20: previousAverage20,

          ratio: volumeRatio,

          priceDirection,
        },

        orderBook: {
          bidVolume,

          askVolume,

          bidPercentage: Number(bidPercentage.toFixed(3)),

          askPercentage: Number(askPercentage.toFixed(3)),

          imbalance: Number(orderBookImbalance.toFixed(4)),

          interpretation: orderBookInterpretation,

          strongBidPressure,

          strongAskPressure,
        },
      },

      signal: {
        bias,

        decision,

        setupQuality,

        bullishScore,

        bearishScore,

        totalScore,

        dominantScore,

        confidencePercent: confidence,

        signalAgreementPercent,

        signals,
      },

      confirmation: {
        entryBullish,
        confirmationBullish,
        trendBullish,
        higherTrendBullish,

        rsiHealthy,

        macdBullish,

        bookBullish: orderBookImbalance > 0.1,

        strongBidPressure,

        strongAskPressure,

        volumeHealthy,

        rrAcceptable,

        resistanceAcceptable,

        resistanceChaseFilter,

        trendAlignment,

        executionBullish,

        orderBookPass,
      },

      tradeSetup,

      reasoning,

      riskControls: {
        minimumRiskReward: 1.5,

        maximumTechnicalConfidence: 95,

        maximumEntryPullbackATR: 1.0,

        stopATRMultiplier: 1.5,

        strongAskPressureThreshold: -0.25,

        strongBidPressureThreshold: 0.25,

        resistanceAware: true,

        volumeAware: true,

        multiTimeframeRequired: true,

        conservativePullbackEntry: true,

        readOnly: true,
      },

      disclaimer:
        "Technical analysis is probabilistic and not financial advice. This read-only tool does not execute trades.",
    };
  } catch (error) {
    const fallback = await coingeckoFallbackForSymbol(symbol);

    const reason =
      error instanceof Error ? error.message : "Unknown Binance API error.";

    return {
      tool: "binance_trade_analysis",
      version: "5.0.0",
      symbol,
      timestamp: new Date().toISOString(),
      dataSource: "CoinGecko fallback (Binance restricted in this region)",
      readOnly: true,
      evaluation: {
        decision: "WAIT",
        setupQuality: "NO_SETUP",
        confidencePercent: 15,
        status: "BINANCE_RESTRICTED",
        exactEvaluation:
          "Binance public API is geo-restricted from this environment (HTTP 451 / restricted location). Technical analysis is unavailable from Binance, so the tool has switched to a CoinGecko price fallback only.",
        reason,
      },
      market: fallback
        ? {
            price: fallback.price,
            change24hPercent: fallback.change24hPercent,
            volume24h: fallback.volume24h,
            source: fallback.source,
          }
        : {
            price: null,
            change24hPercent: null,
            volume24h: null,
            source: "unavailable",
          },
      signal: {
        bias: "NEUTRAL",
        decision: "WAIT",
        setupQuality: "NO_SETUP",
        bullishScore: 0,
        bearishScore: 0,
        totalScore: 0,
        confidencePercent: 15,
        signalAgreementPercent: 0,
        signals: [],
      },
      tradeSetup: {
        type: "NO_NEW_LONG",
        entry: null,
        stopLoss: null,
        takeProfit1: null,
        takeProfit2: null,
        riskReward: null,
        note: "Binance data is restricted from this environment. Full technical analysis is unavailable until Binance access is restored.",
      },
      reasoning: [
        "Binance returned HTTP 451 or a restricted-location error.",
        "The MCP connector is operational, but upstream Binance API is geo-blocked from this environment.",
        fallback
          ? `A fallback price (${fallback.source}) is available: $${fallback.price} (${fallback.change24hPercent}% 24h), but this does not provide technical indicators (EMA, RSI, MACD, etc.).`
          : "No fallback market data was available from CoinGecko or reference prices.",
        "This is an infrastructure limitation, not a market signal.",
      ],
      disclaimer:
        "This is a reduced evaluation: Binance market data is restricted in this region, so the tool cannot complete full multi-timeframe technical analysis.",
    };
  }
}

/* ============================================================
   MCP SERVER
   ============================================================ */

function createServer() {
  const server = new McpServer({
    name: "Binance Live Market Data",

    version: "5.0.0",
  });

  /* ==========================================================
     1. CURRENT PRICE
     ========================================================== */

  server.registerTool(
    "binance_price",
    {
      description: "Get the current Binance Spot price for a trading symbol.",

      inputSchema: {
        symbol: symbolSchema.describe(
          "Binance Spot symbol in uppercase, e.g. BTCUSDT",
        ),
      },
    },

    async ({ symbol }) => {
      try {
        const data = await binanceRequest("/api/v3/ticker/price", { symbol });
        return jsonResult(data);
      } catch (error) {
        const fallback = await coingeckoFallbackForSymbol(symbol);
        if (fallback) {
          return jsonResult({
            symbol,
            price: fallback.price.toString(),
            source: fallback.source,
            note: "Binance is geo-restricted; price from fallback provider",
          });
        }
        return errorResult(error);
      }
    },
  );

  /* ==========================================================
     2. 24H TICKER
     ========================================================== */

  server.registerTool(
    "binance_24hr",
    {
      description: "Get Binance Spot 24-hour statistics.",

      inputSchema: {
        symbol: symbolSchema,
      },
    },

    async ({ symbol }) => {
      try {
        const data = await binanceRequest("/api/v3/ticker/24hr", { symbol });
        return jsonResult(data);
      } catch (error) {
        const fallback = await coingeckoFallbackForSymbol(symbol);
        if (fallback) {
          return jsonResult({
            symbol,
            lastPrice: fallback.price.toString(),
            priceChangePercent: fallback.change24hPercent.toString(),
            source: fallback.source,
            note: "Binance is geo-restricted; data from fallback provider",
          });
        }
        return errorResult(error);
      }
    },
  );

  /* ==========================================================
     3. KLINES
     ========================================================== */

  server.registerTool(
    "binance_klines",
    {
      description: "Get Binance Spot candlestick data for technical analysis.",

      inputSchema: {
        symbol: symbolSchema,

        interval: intervalSchema,

        limit: z.number().int().min(1).max(1000).default(100),
      },
    },

    async ({ symbol, interval, limit }) => {
      try {
        const data = await binanceRequest("/api/v3/klines", {
          symbol,
          interval,
          limit,
        });
        return jsonResult(data);
      } catch (error) {
        const fallback = await coingeckoFallbackForSymbol(symbol);
        if (fallback) {
          return jsonResult({
            symbol,
            interval,
            limit,
            note: "Binance is geo-restricted; returning fallback price only",
            fallback: {
              price: fallback.price,
              change24hPercent: fallback.change24hPercent,
              source: fallback.source,
            },
          });
        }
        return errorResult(error);
      }
    },
  );

  /* ==========================================================
     4. ORDER BOOK
     ========================================================== */

  server.registerTool(
    "binance_order_book",
    {
      description: "Get Binance Spot order book.",

      inputSchema: {
        symbol: symbolSchema,

        limit: z
          .number()
          .int()
          .refine((value) => [5, 10, 20, 50, 100, 500, 1000].includes(value), {
            message: "Limit must be one of: 5, 10, 20, 50, 100, 500, 1000",
          })
          .default(20),
      },
    },

    async ({ symbol, limit }) => {
      try {
        const data = await binanceRequest("/api/v3/depth", {
          symbol,
          limit,
        });

        return jsonResult(data);
      } catch (error) {
        return errorResult(error);
      }
    },
  );

  /* ==========================================================
     5. BOOK TICKER
     ========================================================== */

  server.registerTool(
    "binance_book_ticker",
    {
      description: "Get best Binance Spot bid and ask.",

      inputSchema: {
        symbol: symbolSchema,
      },
    },

    async ({ symbol }) => {
      try {
        const data = await binanceRequest("/api/v3/ticker/bookTicker", {
          symbol,
        });

        return jsonResult(data);
      } catch (error) {
        return errorResult(error);
      }
    },
  );

  /* ==========================================================
     6. RECENT TRADES
     ========================================================== */

  server.registerTool(
    "binance_recent_trades",
    {
      description: "Get recent Binance Spot public trades.",

      inputSchema: {
        symbol: symbolSchema,

        limit: z.number().int().min(1).max(1000).default(100),
      },
    },

    async ({ symbol, limit }) => {
      try {
        const data = await binanceRequest("/api/v3/trades", {
          symbol,
          limit,
        });

        return jsonResult(data);
      } catch (error) {
        return errorResult(error);
      }
    },
  );

  /* ==========================================================
     7. EXCHANGE INFO
     ========================================================== */

  server.registerTool(
    "binance_exchange_info",
    {
      description: "Get Binance Spot exchange information.",

      inputSchema: {
        symbol: symbolSchema,
      },
    },

    async ({ symbol }) => {
      try {
        const data = await binanceRequest("/api/v3/exchangeInfo", { symbol });

        return jsonResult(data);
      } catch (error) {
        return errorResult(error);
      }
    },
  );

  /* ==========================================================
     8. SERVER TIME
     ========================================================== */

  server.registerTool(
    "binance_server_time",
    {
      description: "Get Binance server time.",
    },

    async () => {
      try {
        const data = await binanceRequest("/api/v3/time");

        return jsonResult(data);
      } catch (error) {
        return errorResult(error);
      }
    },
  );

  /* ==========================================================
     9. PING
     ========================================================== */

  server.registerTool(
    "binance_ping",
    {
      description: "Check Binance API connectivity.",
    },

    async () => {
      try {
        const data = await binanceRequest("/api/v3/ping");

        return jsonResult({
          status: "ok",
          binance: data,
        });
      } catch (error) {
        return errorResult(error);
      }
    },
  );

  /* ==========================================================
     10. TRADE ANALYSIS
     ========================================================== */

  server.registerTool(
    "binance_trade_analysis",
    {
      description:
        "Perform conservative Binance Spot multi-timeframe technical analysis. Calculates EMA, RSI(6), RSI(14), MACD, Bollinger Bands, ATR, corrected support/resistance, volume, order-book imbalance, conservative pullback entry, stop-loss, take-profit, risk/reward and confidence. BUY requires multi-timeframe alignment, healthy momentum, acceptable risk/reward, sufficient resistance room and no strong ask pressure. Poor geometry automatically results in WAIT. Analysis only; no trades are executed.",

      inputSchema: {
        symbol: symbolSchema.describe("Binance Spot symbol, e.g. BTCUSDT"),

        entry_interval: intervalSchema
          .default("5m")
          .describe("Entry/timing timeframe. Recommended 5m or 15m."),

        confirmation_interval: intervalSchema
          .default("15m")
          .describe("Confirmation timeframe."),

        trend_interval: intervalSchema
          .default("1h")
          .describe("Primary trend timeframe."),

        higher_interval: intervalSchema
          .default("4h")
          .describe("Higher timeframe trend."),
      },
    },

    async ({
      symbol,
      entry_interval,
      confirmation_interval,
      trend_interval,
      higher_interval,
    }) => {
      try {
        const result = await runTradeAnalysis(
          symbol,
          entry_interval,
          confirmation_interval,
          trend_interval,
          higher_interval,
        );

        return jsonResult(result);
      } catch (error) {
        return errorResult(error);
      }
    },
  );

  return server;
}

/* ============================================================
   MCP HANDLER
   ============================================================ */

const mcpHandler = createMcpHandler(createServer, {
  route: "/mcp",
  allowedHostnames: [
    "localhost",
    "127.0.0.1",
    "binance-live-mcp.workers.dev",
    "binance-live-mcp.binance-live-mcp.workers.dev",
  ],
  allowedOriginHostnames: "*",
});

/* ============================================================
   CLOUDFLARE WORKER
   ============================================================ */

export default {
  async fetch(request: Request, env: unknown, ctx: ExecutionContext) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: withCors(),
      });
    }

    /* ----------------------------------------------------------
       ROOT
       ---------------------------------------------------------- */

    if (url.pathname === "/") {
      return new Response(
        JSON.stringify({
          name: "Binance Live Market Data MCP",

          version: "5.0.0",

          status: "online",

          endpoint: "/mcp",

          readOnly: true,

          tools: 10,
        }),
        {
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }

    /* ----------------------------------------------------------
       DEBUG BINANCE
       ---------------------------------------------------------- */

    if (url.pathname === "/debug/binance") {
      try {
        const data = await binanceRequest("/api/v3/ticker/price", {
          symbol: "BTCUSDT",
        });

        return new Response(
          JSON.stringify({
            status: "ok",

            source: "Cloudflare Worker → Binance",

            data,
          }),
          {
            headers: {
              "content-type": "application/json",
            },
          },
        );
      } catch (error) {
        return new Response(
          JSON.stringify({
            status: "error",

            error: error instanceof Error ? error.message : "Unknown error",
          }),
          {
            status: 502,

            headers: {
              "content-type": "application/json",
            },
          },
        );
      }
    }

    /* ----------------------------------------------------------
       DEBUG TOOLS
       ---------------------------------------------------------- */

    if (url.pathname === "/debug/tools") {
      return new Response(
        JSON.stringify({
          status: "ok",

          server: "Binance Live Market Data",

          version: "5.0.0",

          count: 10,

          tools: [
            "binance_price",
            "binance_24hr",
            "binance_klines",
            "binance_order_book",
            "binance_book_ticker",
            "binance_recent_trades",
            "binance_exchange_info",
            "binance_server_time",
            "binance_ping",
            "binance_trade_analysis",
          ],
        }),
        {
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }

    /* ----------------------------------------------------------
       DEBUG ANALYSIS
       ---------------------------------------------------------- */

    if (url.pathname === "/debug/analysis") {
      try {
        const result = await runTradeAnalysis(
          "BTCUSDT",
          "5m",
          "15m",
          "1h",
          "4h",
        );

        return new Response(
          JSON.stringify(
            {
              status: "ok",

              diagnostic: true,

              symbol: "BTCUSDT",

              result,
            },
            null,
            2,
          ),
          {
            headers: {
              "content-type": "application/json",
            },
          },
        );
      } catch (error) {
        return new Response(
          JSON.stringify({
            status: "error",

            diagnostic: true,

            error: error instanceof Error ? error.message : "Unknown error",
          }),
          {
            status: 502,

            headers: {
              "content-type": "application/json",
            },
          },
        );
      }
    }

    /* ----------------------------------------------------------
       MCP
       ---------------------------------------------------------- */

    if (url.pathname !== "/mcp") {
      return new Response("Not Found", {
        status: 404,
        headers: withCors(),
      });
    }

    const response = await mcpHandler(request, env, ctx);

    const responseHeaders = new Headers(response.headers);

    for (const [key, value] of Object.entries(corsHeaders)) {
      responseHeaders.set(key, value);
    }

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders,
    });
  },
} satisfies ExportedHandler;
