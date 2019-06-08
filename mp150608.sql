create table [ARTICLE]
( 
	[ID]                 integer  not null  identity ( 1,1 ) ,
	[NAME]               varchar(100)  null ,
	[PRICE]              decimal(10,3)  null ,
	[QUANTITY]           integer  null ,
	[SHOP_ID]            integer  null 
)

go

create table [BUYER]
( 
	[ID]                 integer  not null  identity ( 1,1 ) ,
	[NAME]               varchar(100)  null ,
	[CREDIT]             decimal(10,3)  null ,
	[CITY_ID]            integer  null 
)

go

create table [CITY]
( 
	[ID]                 integer  not null  identity ( 1,1 ) ,
	[NAME]               varchar(100)  null 
)

go

create table [LINE]
( 
	[ID]                 integer  not null  identity ( 1,1 ) ,
	[CITY_ID1]           integer  null ,
	[CITY_ID2]           integer  null ,
	[DISTANCE]           integer  null 
)

go

create table [ORDER]
( 
	[ID]                 integer  not null  identity ( 1,1 ) ,
	[STATE]              varchar(100)  null ,
	[SENT_TIME]          datetime  null ,
	[RECEIVED_TIME]      datetime  null ,
	[BUYER_ID]           integer  null 
)

go

create table [ORDER_ITEM]
( 
	[ID]                 integer  not null  identity ( 1,1 ) ,
	[ORDER_ID]           integer  not null ,
	[ARTICLE_ID]         integer  not null ,
	[QUANTITY]           integer  null ,
	[DISCOUNT]           integer  null 
)

go

create table [SHOP]
( 
	[ID]                 integer  not null  identity ( 1,1 ) ,
	[NAME]               varchar(100)  null ,
	[CREDIT]             decimal(10,3)  null ,
	[CITY_ID]            integer  null ,
	[DISCOUNT]           integer  null 
)

go

create table [SYSTEM]
( 
	[CURRENT_DATE]       datetime  null 
)

go

create table [TRANSACTION]
( 
	[ID]                 integer  not null  identity ( 1,1 ) ,
	[TYPE]               integer  null ,
	[SENDER]             integer  null ,
	[RECEIVER]           integer  null ,
	[EXECUTION_TIME]     datetime  null ,
	[AMOUNT]             decimal(10,3)  null ,
	[ORDER_ID]           integer  null 
)

go

create table [TRAVELING]
( 
	[ID]                 integer  not null  identity ( 1,1 ) ,
	[START_DATE]         datetime  null ,
	[LINE_ID]            integer  null ,
	[ORDER_ID]           integer  null ,
	[DIRECTION]          integer  null 
)

go

create or alter procedure SP_TRUNCATE_ALL_TABLES
as
begin

truncate table article
truncate table buyer
truncate table city
truncate table line
truncate table [order]
truncate table order_item
truncate table shop
truncate table [system]
truncate table [transaction]
truncate table traveling

end

go

create or alter procedure SP_FINAL_PRICE @order_id int, @final_price decimal(10,3) output
as
begin
	declare @c cursor;
	declare @price decimal(10,3);
	declare @discount int;
	declare @quantity int;
	declare @d decimal(10,3);

	set @c = cursor for (select A.PRICE, OI.DISCOUNT, OI.QUANTITY from 
	(select [ARTICLE].ID, [ARTICLE].PRICE, [SHOP].DISCOUNT from [ARTICLE] join [SHOP] on [ARTICLE].SHOP_ID = [SHOP].ID) as A 
	join (select * from [ORDER_ITEM] where ORDER_ID = @order_id) as OI 
	on A.ID = OI.ARTICLE_ID);

	open @c;
	fetch next from @c into @price, @discount, @quantity;

	set @final_price = 0;
	while @@FETCH_STATUS = 0
	begin
		set @d = 1 -  @discount / 100.0;
		set @final_price = @final_price + @price * @quantity * @d;

		fetch next from @c into @price, @discount, @quantity;
	end
	
	declare @until datetime;
	declare @from datetime;
	select @until = [CURRENT_DATE] from [SYSTEM];
	set @from = dateadd(day, -30, @until);

	declare @buyer int;
	select @buyer = BUYER_ID from [ORDER] where ID = @order_id;

	declare @recentPurchaseAmount decimal;

	set @recentPurchaseAmount = 0;
	select @recentPurchaseAmount = sum(AMOUNT) from [TRANSACTION] 
	where [TYPE] = 1 and ORDER_ID <> @order_id and SENDER = @buyer and EXECUTION_TIME BETWEEN @from AND @until

	if(@recentPurchaseAmount >= 10000.000) set @final_price = @final_price * 0.98;

	close @c;
	deallocate @c;
end

go

create or alter procedure SP_DISCOUNT_SUM @order_id int, @discount_sum decimal(10,3) output
as
begin
	declare @c cursor;
	declare @price decimal(10,3);
	declare @discount int;
	declare @quantity int;
	declare @d decimal(10,3);
	declare @final_price decimal(10, 3);
	declare @total_sum decimal(10, 3);

	set @c = cursor for (select A.PRICE, OI.DISCOUNT, OI.QUANTITY from 
	(select [ARTICLE].ID, [ARTICLE].PRICE, [SHOP].DISCOUNT from [ARTICLE] join [SHOP] on [ARTICLE].SHOP_ID = [SHOP].ID) as A 
	join (select * from [ORDER_ITEM] where ORDER_ID = @order_id) as OI 
	on A.ID = OI.ARTICLE_ID);

	open @c;
	fetch next from @c into @price, @discount, @quantity;

	set @final_price = 0;
	set @total_sum = 0;
	while @@FETCH_STATUS = 0
	begin
		set @d = 1 -  @discount / 100.0;
		set @final_price = @final_price + @price * @quantity * @d;
		set @total_sum = @total_sum + @price * @quantity;

		fetch next from @c into @price, @discount, @quantity;
	end
	
	declare @until datetime;
	declare @from datetime;
	select @until = [CURRENT_DATE] from [SYSTEM];
	set @from = dateadd(day, -30, @until);

	declare @buyer int;
	select @buyer = BUYER_ID from [ORDER] where ID = @order_id;

	declare @recentPurchaseAmount decimal;

	set @recentPurchaseAmount = 0;
	select @recentPurchaseAmount = sum(AMOUNT) from [TRANSACTION] 
	where [TYPE] = 1 and ORDER_ID <> @order_id and SENDER = @buyer and EXECUTION_TIME BETWEEN @from AND @until

	if(@recentPurchaseAmount >= 10000.000) set @final_price = @final_price * 0.98;

	set @discount_sum = @total_sum - @final_price; 

	close @c;
	deallocate @c;
end

go

create or alter procedure SP_SYSTEM_TRANSACTIONS @order_id int, @final_price decimal(10,3) output, @system_cut decimal(10,3) output
as
begin
	declare @c cursor;
	declare @price decimal(10,3);
	declare @discount int;
	declare @quantity int;
	declare @d decimal(10,3);

	set @c = cursor for (select A.PRICE, OI.DISCOUNT, OI.QUANTITY from 
	(select [ARTICLE].ID, [ARTICLE].PRICE, [SHOP].DISCOUNT from [ARTICLE] join [SHOP] on [ARTICLE].SHOP_ID = [SHOP].ID) as A 
	join (select * from [ORDER_ITEM] where ORDER_ID = @order_id) as OI 
	on A.ID = OI.ARTICLE_ID);

	open @c;
	fetch next from @c into @price, @discount, @quantity;

	set @final_price = 0;
	while @@FETCH_STATUS = 0
	begin
		set @d = 1 -  @discount / 100.0;
		set @final_price = @final_price + @price * @quantity * @d;

		fetch next from @c into @price, @discount, @quantity;
	end
	
	declare @until datetime;
	declare @from datetime;
	select @until = [CURRENT_DATE] from [SYSTEM];
	set @from = dateadd(day, -30, @until);

	declare @buyer int;
	select @buyer = BUYER_ID from [ORDER] where ID = @order_id;

	declare @recentPurchaseAmount decimal;

	set @recentPurchaseAmount = 0;
	select @recentPurchaseAmount = sum(AMOUNT) from [TRANSACTION] 
	where [TYPE] = 1 and ORDER_ID <> @order_id and SENDER = @buyer and EXECUTION_TIME BETWEEN @from AND @until

	if(@recentPurchaseAmount >= 10000.000) 
	begin
		set @system_cut = @final_price * 0.03;
		set @final_price = @final_price * 0.98;
	end
	else
	begin
		set @system_cut = @final_price * 0.05;
	end

	close @c;
	deallocate @c;
end

go

create or alter trigger TR_TIME_UPDATE
on [SYSTEM]
for update
as
begin
	declare @newDate datetime;
	declare @receivedTime datetime;
	declare @countTraveling int;
	declare @orderId int;
	declare @orderCursor cursor;

	select @newDate = [CURRENT_DATE] from inserted;

	set @orderCursor = cursor for (select ID from [ORDER] where RECEIVED_TIME is null);

	open @orderCursor;
	fetch next from @orderCursor into @orderId;

	while @@FETCH_STATUS = 0
	begin
		select @countTraveling = count(*) from TRAVELING join LINE on LINE_ID = LINE.ID where [ORDER_ID] = @orderId and @newDate < dateadd(day, DISTANCE, [START_DATE]);
		if(@countTraveling = 0) 
		begin
			select top(1) @receivedTime = dateadd(day, DISTANCE, [START_DATE]) from [TRAVELING] T join LINE L on T.LINE_ID = L.ID order by [START_DATE] desc
			update [ORDER] set RECEIVED_TIME = @receivedTime, [STATE] = 'arrived' where ID = @orderId and RECEIVED_TIME is null;
		end
		fetch next from @orderCursor into @orderId;
	end

	close @orderCursor;
	deallocate @orderCursor;
end

go

create or alter trigger TR_TRANSFER_MONEY_TO_SHOPS
on [ORDER]
for update
as
begin
	declare @c cursor;
	declare @orderId int;
	declare @shopId int;
	declare @payout decimal(10,3);
	declare @receivedTime datetime;
	
	if update (RECEIVED_TIME) 
	begin
		select @orderId = ID, @receivedTime = RECEIVED_TIME from inserted;

		set @c = cursor for (select A.SHOP_ID, sum(OI.QUANTITY * A.PRICE * (1 - OI.DISCOUNT / 100.0)) as PAYOUT 
		from ORDER_ITEM as OI join ARTICLE as A on OI.ARTICLE_ID = A.ID 
		where OI.ORDER_ID = @orderId 
		group by A.SHOP_ID);

		open @c;
		fetch next from @c into @shopId, @payout;

		while @@FETCH_STATUS = 0
		begin
			insert into [TRANSACTION] values (2, -1, @shopId, @receivedTime, @payout * 0.95, @orderId);

			fetch next from @c into @shopId, @payout;
		end

		close @c;
		deallocate @c;
	end
end

go