package main.java.com.odyssean.pojo;

import java.util.Comparator;

public class PNLRawData implements Comparable<PNLRawData> {

    private String execution_id;
    private String order_id;
    private String deal_id;
    private String wave_id;
    private String exchange_trade_id;
    private String execution_time;
    private String exchange_execution_id;
    private String executed_price;
    private String executed_size;
    private String fees;
    private String fees_currency;
    private String adapter_id;
    private String strategy_id;
    private String account_id;
    private String exchange_order_id;
    private String symbol;
    private String side;
    private String order_price;
    private String order_size;
    private String ccy_1;
    private String ccy_2;
    private String ccy2_src;
    private String underlying_index;
    private String underlying_index_src;
    private String hedge_id;

    private long hedgeId;
    private long waveId;
    private long executionId;

    private String sumCoin;

    // calculated
    private String postTradePositionSize;

    private String tradeCount;
    private String oc;

    private String flipsPosition;
    private String clTradeSizeCTCS;
    private String clTradeSizeCoin;
    private String openingTradeSizeCoins;
    private String cumOpenCoins;
    private String wtdAvg;
    private String RPnLCoin;

    private String belongingPair;


    // Getter Methods

    public String getExecution_id() {
        return execution_id;
    }

    public String getOrder_id() {
        return order_id;
    }

    public String getDeal_id() {
        return deal_id;
    }

    public String getWave_id() {
        return wave_id;
    }

    public String getExchange_trade_id() {
        return exchange_trade_id;
    }

    public String getExecution_time() {
        return execution_time;
    }

    public String getExchange_execution_id() {
        return exchange_execution_id;
    }

    public String getExecuted_price() {
        return executed_price;
    }

    public String getExecuted_size() {
        return executed_size;
    }

    public String getFees() {
        return fees;
    }

    public String getFees_currency() {
        return fees_currency;
    }

    public String getAdapter_id() {
        return adapter_id;
    }

    public String getStrategy_id() {
        return strategy_id;
    }

    public String getAccount_id() {
        return account_id;
    }

    public String getExchange_order_id() {
        return exchange_order_id;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getSide() {
        return side;
    }

    public String getOrder_price() {
        return order_price;
    }

    public String getOrder_size() {
        return order_size;
    }

    public String getCcy_1() {
        return ccy_1;
    }

    public String getCcy_2() {
        return ccy_2;
    }

    public String getCcy2_src() {
        return ccy2_src;
    }

    public String getUnderlying_index() {
        return underlying_index;
    }

    public String getUnderlying_index_src() {
        return underlying_index_src;
    }

    public String getHedge_id() {
        return hedge_id;
    }

    // Setter Methods

    public void setExecution_id(String execution_id) {
        this.execution_id = execution_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public void setDeal_id(String deal_id) {
        this.deal_id = deal_id;
    }

    public void setWave_id(String wave_id) {
        this.wave_id = wave_id;
    }

    public void setExchange_trade_id(String exchange_trade_id) {
        this.exchange_trade_id = exchange_trade_id;
    }

    public void setExecution_time(String execution_time) {
        this.execution_time = execution_time;
    }

    public void setExchange_execution_id(String exchange_execution_id) {
        this.exchange_execution_id = exchange_execution_id;
    }

    public void setExecuted_price(String executed_price) {
        this.executed_price = executed_price;
    }

    public void setExecuted_size(String executed_size) {
        this.executed_size = executed_size;
    }

    public void setFees(String fees) {
        this.fees = fees;
    }

    public void setFees_currency(String fees_currency) {
        this.fees_currency = fees_currency;
    }

    public void setAdapter_id(String adapter_id) {
        this.adapter_id = adapter_id;
    }

    public void setStrategy_id(String strategy_id) {
        this.strategy_id = strategy_id;
    }

    public void setAccount_id(String account_id) {
        this.account_id = account_id;
    }

    public void setExchange_order_id(String exchange_order_id) {
        this.exchange_order_id = exchange_order_id;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public void setSide(String side) {
        this.side = side;
    }

    public void setOrder_price(String order_price) {
        this.order_price = order_price;
    }

    public void setOrder_size(String order_size) {
        this.order_size = order_size;
    }

    public void setCcy_1(String ccy_1) {
        this.ccy_1 = ccy_1;
    }

    public void setCcy_2(String ccy_2) {
        this.ccy_2 = ccy_2;
    }

    public void setCcy2_src(String ccy2_src) {
        this.ccy2_src = ccy2_src;
    }

    public void setUnderlying_index(String underlying_index) {
        this.underlying_index = underlying_index;
    }

    public void setUnderlying_index_src(String underlying_index_src) {
        this.underlying_index_src = underlying_index_src;
    }

    public void setHedge_id(String hedge_id) {
        this.hedge_id = hedge_id;
    }

    @Override
    public String toString() {
        return "PNLRawData{" +
                "execution_id='" + execution_id + '\'' +
                ", order_id='" + order_id + '\'' +
                ", deal_id='" + deal_id + '\'' +
                ", wave_id='" + wave_id + '\'' +
                ", exchange_trade_id='" + exchange_trade_id + '\'' +
                ", execution_time='" + execution_time + '\'' +
                ", exchange_execution_id='" + exchange_execution_id + '\'' +
                ", executed_price='" + executed_price + '\'' +
                ", executed_size='" + executed_size + '\'' +
                ", fees='" + fees + '\'' +
                ", fees_currency='" + fees_currency + '\'' +
                ", adapter_id='" + adapter_id + '\'' +
                ", strategy_id='" + strategy_id + '\'' +
                ", account_id='" + account_id + '\'' +
                ", exchange_order_id='" + exchange_order_id + '\'' +
                ", symbol='" + symbol + '\'' +
                ", side='" + side + '\'' +
                ", order_price='" + order_price + '\'' +
                ", order_size='" + order_size + '\'' +
                ", ccy_1='" + ccy_1 + '\'' +
                ", ccy_2='" + ccy_2 + '\'' +
                ", ccy2_src='" + ccy2_src + '\'' +
                ", underlying_index='" + underlying_index + '\'' +
                ", underlying_index_src='" + underlying_index_src + '\'' +
                ", hedge_id='" + hedge_id + '\'' +
                ", hedgeId=" + hedgeId +
                ", waveId=" + waveId +
                ", executionId=" + executionId +
                ", postTradePositionSize='" + postTradePositionSize + '\'' +
                ", tradeCount='" + tradeCount + '\'' +
                ", oc='" + oc + '\'' +
                ", flipsPosition='" + flipsPosition + '\'' +
                ", clTradeSizeCTCS='" + clTradeSizeCTCS + '\'' +
                ", clTradeSizeCoin='" + clTradeSizeCoin + '\'' +
                ", openingTradeSizeCoins='" + openingTradeSizeCoins + '\'' +
                ", cumOpenCoins='" + cumOpenCoins + '\'' +
                ", wtdAvg='" + wtdAvg + '\'' +
                ", RPnLCoin='" + RPnLCoin + '\'' +
                '}';
    }

    public long getHedgeId() {
        return hedgeId;
    }

    public void setHedgeId(long hedgeId) {
        this.hedgeId = hedgeId;
    }

    public long getWaveId() {
        return waveId;
    }

    public void setWaveId(long waveId) {
        this.waveId = waveId;
    }

    public long getExecutionId() {
        return executionId;
    }

    public void setExecutionId(long executionId) {
        this.executionId = executionId;
    }

    @Override
    public int compareTo(PNLRawData o){
        return Comparator.comparing(PNLRawData::getBelongingPair)
                .thenComparing(PNLRawData::getWaveId)
                .thenComparing(PNLRawData::getExecutionId)
                .compare(this, o);
    }

    public String getPostTradePositionSize() {
        return postTradePositionSize;
    }

    public void setPostTradePositionSize(String postTradePositionSize) {
        this.postTradePositionSize = postTradePositionSize;
    }

    public String getTradeCount() {
        return tradeCount;
    }

    public void setTradeCount(String tradeCount) {
        this.tradeCount = tradeCount;
    }

    public String getOc() {
        return oc;
    }

    public void setOc(String oc) {
        this.oc = oc;
    }

    public String getFlipsPosition() {
        return flipsPosition;
    }

    public void setFlipsPosition(String flipsPosition) {
        this.flipsPosition = flipsPosition;
    }

    public String getClTradeSizeCTCS() {
        return clTradeSizeCTCS;
    }

    public void setClTradeSizeCTCS(String clTradeSizeCTCS) {
        this.clTradeSizeCTCS = clTradeSizeCTCS;
    }

    public String getClTradeSizeCoin() {
        return clTradeSizeCoin;
    }

    public void setClTradeSizeCoin(String clTradeSizeCoin) {
        this.clTradeSizeCoin = clTradeSizeCoin;
    }

    public String getOpeningTradeSizeCoins() {
        return openingTradeSizeCoins;
    }

    public void setOpeningTradeSizeCoins(String openingTradeSizeCoins) {
        this.openingTradeSizeCoins = openingTradeSizeCoins;
    }

    public String getCumOpenCoins() {
        return cumOpenCoins;
    }

    public void setCumOpenCoins(String cumOpenCoins) {
        this.cumOpenCoins = cumOpenCoins;
    }

    public String getWtdAvg() {
        return wtdAvg;
    }

    public void setWtdAvg(String wtdAvg) {
        this.wtdAvg = wtdAvg;
    }

    public String getRPnLCoin() {
        return RPnLCoin;
    }

    public void setRPnLCoin(String RPnLCoin) {
        this.RPnLCoin = RPnLCoin;
    }

    public String getSumCoin() {
        return sumCoin;
    }

    public void setSumCoin(String sumCoin) {
        this.sumCoin = sumCoin;
    }


    public String getBelongingPair() {
        return belongingPair;
    }

    public void setBelongingPair(String belongingPair) {
        this.belongingPair = belongingPair;
    }


}
