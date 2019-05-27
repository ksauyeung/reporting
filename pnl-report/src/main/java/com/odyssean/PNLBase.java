package main.java.com.odyssean;

import main.java.com.odyssean.pojo.PNLRawData;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class PNLBase {


    private static HashMap<String, String> sizesMap = new HashMap<>();

    private static final String OKEXFUTURE_ADAPTER = "OKEXFUTURE_ADAPTER";
    private static final String DERIBIT_ADAPTER = "DERIBIT_ADAPTER";
    private static final String BITMEX_ADAPTER = "BITMEX_ADAPTER";

    private static HashSet<String> spotPairs = new HashSet<>();

    static {

        // ours
        spotPairs.add("ETHUSD");
        spotPairs.add("BTCUSD");
        spotPairs.add("XRPUSD");
        spotPairs.add("ETCUSD");

        // random
        spotPairs.add("EOSUSD");
        spotPairs.add("LTCUSD");
        spotPairs.add("BCHUSD");
        spotPairs.add("XLMUSD");

    }

    protected static BigDecimal getRpnlSum(PNLRawData data) {

        String adapter = data.getAdapter_id();
        String symobl = data.getSymbol();

        if (!(adapter.equalsIgnoreCase(OKEXFUTURE_ADAPTER)
                || adapter.equalsIgnoreCase(DERIBIT_ADAPTER)
                || adapter.equalsIgnoreCase(BITMEX_ADAPTER))){
            adapter = "";
        }


        for (Map.Entry<String, String> e : sizesMap.entrySet()) {
            if ((adapter+symobl).startsWith(e.getKey().replace("*",""))) {
                return new BigDecimal(e.getValue());
            }
        }
        return new BigDecimal("1");

    }

    protected static void buildRpnlSumMap() {

        // okex
        sizesMap.put(OKEXFUTURE_ADAPTER+"BTC*","100");
        sizesMap.put(OKEXFUTURE_ADAPTER+"ETH*","10");
        sizesMap.put(OKEXFUTURE_ADAPTER+"ETC*","10");
        sizesMap.put(OKEXFUTURE_ADAPTER+"XRP*","10");
        sizesMap.put(OKEXFUTURE_ADAPTER+"EOS*","10");
        sizesMap.put(OKEXFUTURE_ADAPTER+"BCH*","10");
        sizesMap.put(OKEXFUTURE_ADAPTER+"BSV*","10");

        // bitmex
        sizesMap.put(BITMEX_ADAPTER+"XBT*","1");
        sizesMap.put(BITMEX_ADAPTER+"ETHUSD","1");
        sizesMap.put(BITMEX_ADAPTER+"ETH*","1");
        sizesMap.put(BITMEX_ADAPTER+"ADA*","1");
        sizesMap.put(BITMEX_ADAPTER+"BCH*","1");
        sizesMap.put(BITMEX_ADAPTER+"EOS*","1");
        sizesMap.put(BITMEX_ADAPTER+"LTC*","1");
        sizesMap.put(BITMEX_ADAPTER+"XRP*","1");

        // derabit
        sizesMap.put(DERIBIT_ADAPTER +"BTC*","10");
        sizesMap.put(DERIBIT_ADAPTER +"ETH*","1");

        // CRYPTOFACILITIES
        sizesMap.put("PI_*","1");
        sizesMap.put("FI_*","1");
        sizesMap.put("PV_XRPXBT*","1");
        sizesMap.put("FV_XRPXBT*","1");

    }

    public static String replaceNull(String input) {
        return input == null ? "" : input;
    }

    protected static boolean isSpot(PNLRawData data){
        String symbol = data.getSymbol();
        return spotPairs.contains(symbol);
    }


    ////// ignore below


    /*


execution_id,order_id,deal_id,wave_id,exchange_trade_id,execution_time,exchange_execution_id,executed_price,executed_size,fees,fees_currency,adapter_id,strategy_id,account_id,exchange_order_id,symbol,side,order_price,order_size,ccy_1,ccy_2,ccy2_src,underlying_index,underlying_index_src,hedge_id
1,1,621,34040,,"2018-11-22 13:13:33",2,5.514,4.0,0.00217628,ETC,OKEXFUTURE_ADAPTER,MANUAL-621,0,1845258712730624,ETC1123,SELL,0.0,5.0,0.0,0.0,,5.52862189,OKEXFUTURE_ADAPTER.etc_usd,1130

[
  {
    "execution_id": "1",
    "order_id": "1",
    "deal_id": "621",
    "wave_id": "34040",
    "exchange_trade_id": "",
    "execution_time": "2018-11-22 13:13:33",
    "exchange_execution_id": "2",
    "executed_price": "5.514",
    "executed_size": "4.0",
    "fees": "0.00217628",
    "fees_currency": "ETC",
    "adapter_id": "OKEXFUTURE_ADAPTER",
    "strategy_id": "MANUAL-621",
    "account_id": "0",
    "exchange_order_id": "1845258712730624",
    "symbol": "ETC1123",
    "side": "SELL",
    "order_price": "0.0",
    "order_size": "5.0",
    "ccy_1": "0.0",
    "ccy_2": "0.0",
    "ccy2_src": "",
    "underlying_index": "5.52862189",
    "underlying_index_src": "OKEXFUTURE_ADAPTER.etc_usd",
    "hedge_id": "1130"
  }
]

     */


}
