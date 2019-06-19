CREATE TABLE [ARTICLE]
( 
	[ID]                 integer  NOT NULL  IDENTITY ( 1,1 ) ,
	[NAME]               varchar(100)  NULL ,
	[PRICE]              decimal(10,3)  NULL ,
	[QUANTITY]           integer  NULL ,
	[SHOP_ID]            integer  NULL 
)

go

CREATE TABLE [BUYER]
( 
	[ID]                 integer  NOT NULL  IDENTITY ( 1,1 ) ,
	[NAME]               varchar(100)  NULL ,
	[CREDIT]             decimal(10,3)  NULL ,
	[CITY_ID]            integer  NULL 
)

go

CREATE TABLE [CITY]
( 
	[ID]                 integer  NOT NULL  IDENTITY ( 1,1 ) ,
	[NAME]               varchar(100)  NULL 
)

go

CREATE TABLE [LINE]
( 
	[ID]                 integer  NOT NULL  IDENTITY ( 1,1 ) ,
	[CITY_ID1]           integer  NULL ,
	[CITY_ID2]           integer  NULL ,
	[DISTANCE]           integer  NULL 
)

go

CREATE TABLE [ORDER]
( 
	[ID]                 integer  NOT NULL  IDENTITY ( 1,1 ) ,
	[STATE]              varchar(100)  NULL ,
	[SENT_TIME]          datetime  NULL ,
	[ASSEMBLY_TIME]      datetime  NULL ,
	[RECEIVED_TIME]      datetime  NULL ,
	[BUYER_ID]           integer  NULL 
)

go

CREATE TABLE [ORDER_ITEM]
( 
	[ID]                 integer  NOT NULL  IDENTITY ( 1,1 ) ,
	[ORDER_ID]           integer  NOT NULL ,
	[ARTICLE_ID]         integer  NOT NULL ,
	[QUANTITY]           integer  NULL ,
	[DISCOUNT]           integer  NULL 
)

go

CREATE TABLE [SHOP]
( 
	[ID]                 integer  NOT NULL  IDENTITY ( 1,1 ) ,
	[NAME]               varchar(100)  NULL ,
	[CREDIT]             decimal(10,3)  NULL ,
	[CITY_ID]            integer  NULL ,
	[DISCOUNT]           integer  NULL 
)

go

CREATE TABLE [SYSTEM]
( 
	[CURRENT_DATE]       datetime  NULL 
)

go

CREATE TABLE [TRANSACTION]
( 
	[ID]                 integer  NOT NULL  IDENTITY ( 1,1 ) ,
	[TYPE]               integer  NULL ,
	[SENDER]             integer  NULL ,
	[RECEIVER]           integer  NULL ,
	[EXECUTION_TIME]     datetime  NULL ,
	[AMOUNT]             decimal(10,3)  NULL ,
	[ORDER_ID]           integer  NULL 
)

go

CREATE TABLE [TRAVELING]
( 
	[ID]                 integer  NOT NULL  IDENTITY ( 1,1 ) ,
	[START_DATE]         datetime  NULL ,
	[LINE_ID]            integer  NULL ,
	[ORDER_ID]           integer  NULL ,
	[DIRECTION]          integer  NULL 
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
	declare @buyer int;

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
	select @until = SENT_TIME, @buyer = BUYER_ID from [ORDER] where ID = @order_id;
	if(@until is null) select @until = [CURRENT_DATE] from [SYSTEM];
	set @from = dateadd(day, -30, @until);

	declare @recentPurchaseAmount decimal;

	set @recentPurchaseAmount = 0;
	select @recentPurchaseAmount = sum(AMOUNT) from [TRANSACTION] 
	where [TYPE] = 1 and ORDER_ID < @order_id and SENDER = @buyer and EXECUTION_TIME BETWEEN @from AND @until

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
	declare @buyer int;

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
	select @until = SENT_TIME, @buyer = BUYER_ID from [ORDER] where ID = @order_id;
	if(@until is null) select @until = [CURRENT_DATE] from [SYSTEM];
	set @from = dateadd(day, -30, @until);

	declare @recentPurchaseAmount decimal;

	set @recentPurchaseAmount = 0;
	select @recentPurchaseAmount = sum(AMOUNT) from [TRANSACTION] 
	where [TYPE] = 1 and ORDER_ID < @order_id and SENDER = @buyer and EXECUTION_TIME BETWEEN @from AND @until

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
	declare @buyer int;

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
	select @until = SENT_TIME, @buyer = BUYER_ID from [ORDER] where ID = @order_id;
	if(@until is null) select @until = [CURRENT_DATE] from [SYSTEM];
	set @from = dateadd(day, -30, @until);

	declare @recentPurchaseAmount decimal;

	set @recentPurchaseAmount = 0;
	select @recentPurchaseAmount = sum(AMOUNT) from [TRANSACTION] 
	where [TYPE] = 1 and ORDER_ID < @order_id and SENDER = @buyer and EXECUTION_TIME BETWEEN @from AND @until

	if(@recentPurchaseAmount >= 10000.000) 
	begin
		set @system_cut = @final_price * 0.03;
		set @final_price = @final_price * 0.97;
	end
	else
	begin
		set @system_cut = @final_price * 0.05;
		set @final_price = @final_price * 0.95;
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
	declare @assemblyDate datetime;
	declare @countTraveling int;
	declare @orderId int;
	declare @orderCursor cursor;

	select @newDate = [CURRENT_DATE] from inserted;

	set @orderCursor = cursor for (select ID, ASSEMBLY_TIME from [ORDER] where RECEIVED_TIME is null);

	open @orderCursor;
	fetch next from @orderCursor into @orderId, @assemblyDate;

	while @@FETCH_STATUS = 0
	begin
		select @countTraveling = count(*) from TRAVELING where ORDER_ID = @orderId;
		if(@countTraveling = 0)
		begin
			if(@newDate >= @assemblyDate)
				begin
					update [ORDER] set RECEIVED_TIME = @assemblyDate, [STATE] = 'arrived' where ID = @orderId;
				end
		end
		else
		begin
			select @countTraveling = count(*) from TRAVELING join LINE on LINE_ID = LINE.ID where [ORDER_ID] = @orderId and @newDate < dateadd(day, DISTANCE, [START_DATE]);
			if(@countTraveling = 0) 
			begin
				select top(1) @receivedTime = dateadd(day, DISTANCE, [START_DATE]) from [TRAVELING] T join LINE L on T.LINE_ID = L.ID where [ORDER_ID] = @orderId order by [START_DATE] desc
				update [ORDER] set RECEIVED_TIME = @receivedTime, [STATE] = 'arrived' where ID = @orderId;
			end
		end
		fetch next from @orderCursor into @orderId, @assemblyDate;
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
	declare @recentPurchaseAmount decimal(10,3);
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
			insert into [TRANSACTION] values (2, null, @shopId, @receivedTime, @payout * 0.95, @orderId);

			fetch next from @c into @shopId, @payout;
		end

		close @c;
		deallocate @c;
	end
end

go