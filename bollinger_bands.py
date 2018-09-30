from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, SimpleMovingAverage
from quantopian.pipeline.filters.morningstar import Q1500us
from quantopian.pipeline.data import morningstar





def initialize(context):
    schedule_function(my_rebalance, date_rules.week_start(), time_rules.market_open(hours=1))
    my_pipe = make_pipeline()
    attach_pipeline = (my_pipe, 'my_pipeline')




######################
#       Scanner      #
#    Main Pipeline   #
######################
def make_pipeline():
    # PIPE ( 1 )
    # queries quantopians q1500us read more for details on filter
    base_universe_pipe = Q1500us()
    # PIPE ( 2 )
    # filters for the energy sector
    energy_sector_pipe = sector.eq(309)
    # PIPE ( 3 )
    # filter 1500US and Energy Sector
    base_energy_pipe = base_universe_pipe & energy_sector_pipe
    # PIPE ( 4 )
    # queries Dollar Volume (30 Days) Grab the Data
    dollar_volume_pipe = AverageDollarVolume(window_length=30)
    # PIPE ( 5 )
    # Filters highest 10% in dollar_volume_pipe
    high_dollar_volume_pipe = dollar_volume_pipe.percentile_between(90,100)
    # PIPE ( 6 )
    # Filters top 50 results based on the open value in high_dollar_volume set
    top_open_prices_pipe = USEquityPricing.open.latest.top(50, mask = high_dollar_volume_pipe)
    # PIPE ( 7 )
    # Filters highest 10% based on the close value in top_open_prices_pipe
    high_close_price_pipe = USEquityPricing.close.latest.percentile_between(90, 100, mask = top_open_prices_pipe)
    # PIPE ( 8 )
    # Combine the filters (pipes)
    top_ten_base_energy_pipe = base_energy_pipe & high_dollar_volume_pipe
    # PIPE ( 9 )
    # (10 Day) Mean Close
    mean_10 = SimpleMovingAverage(inputs =
                                  [USEquityPricing.close],
                                   window_length=10,
                                   mask=top_ten_base_energy_pipe)

    # (30 Day) Mean Close
    mean_30 = SimpleMovingAverage(inputs =
                                    [USEquityPricing.close],
                                     window_length = 30,
                                     mask = top_ten_base_energy_pipe)

    # Percentage Difference
    percent_difference = (mean_10 - mean_30) / mean_30

    # Execute trade on short condition to List of Shorts
    shorts = percent_difference < 0

    # Execute trade on long condition to List of Longs
    longs = percent_difference > 0

    # Final Pipe filter for anything in Shorts or Longs (Lists)
    securities_to_trade = (shorts | longs)
    # Return the pipeline



    #######################
    #   Bollinger Bands   #
    #######################
    df['Close: 20 Day Mean'] = df['Close'].rolling(20).mean()
    # Upper Band = 20MA + 2*std(20)
    df['UpperBand'] = df['Close: 20 Day Mean'] + 2*(df['Close'].rolling(20).std())
    # Lower Band = 20MA - 2*std(20)
    df['LowerBand'] = df['Close: 20 Day Mean'] - 2*(df['Close'].rolling(20).std())

    # Graph that whole party 
    df[['Close', 'Close: 20 Day Mean', 'Upper', 'Lower']].plot(figsize=(16,6))




    return Pipeline(columns =
                    {'longs': longs,
                    'shorts': shorts,
                    'perc_diff': percent_difference },
                     screen = securities_to_trade)


######################
#  Trading Strategy  #
#     Passing In     #
#    Main Pipeline   #
######################
def before_trading_start(context):
    context.output = pipline_output('my_pipeline')
    print(context.output)
    # Data Series of booleans

    context.longs = context.output[context.output['longs']].index.tolist()
    context.shorts = context.output[context.output['shorts']].index.tolist()

    context.long_weight, context.short_weight = my_compute_weights(context)




######################
# Account Allocation #
#    Set User Cost   #
######################
def my_compute_weights(context):
    """
    if else statements are filtering for zero division errors:
    """
    if len(context.longs) == 0:
        long_weight = 0
    else:
        long_weight = 0.5 / len(context.longs)

    if len(context.shorts) == 0:
        short_weight = 0
    else:
        short_weight = 0.5 / len(context.shorts)

    return (long_weight, short_weight)



######################
#  Execute Strategy  #
# Garbage Collection #
######################
def my_rebalance(contex, data):
     """
     my_rebalance cleans the pipelines as data changes.
     If there is a security in our portfolio that has fallen out of the conditions
     that its previously passed through then exit any position that we are in on that particular
     security
     """
    for security in context.portfolio.positions:
        if security not in context.longs and security not in context.shorts and data.can_trade(security):
            order_target_percent(security, 0)

    for security in context.longs:
        if data.can_trade(security):
            order_target_percent(security, context.long_weight)

    for security in context.shorts:
        if data.can_trade(security):
            order_target_percent(security, context.short_weight)





result = run_pipeline(make_pipeline(), '2010-01-01', '2018-09-25')

# DataFrame ::: Index = Ticker ::: Columns = longs, shorts, percent_difference
# longs and shorts columns are booleans
print result.Head()
