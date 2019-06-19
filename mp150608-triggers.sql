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
