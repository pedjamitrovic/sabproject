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
