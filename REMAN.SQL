
    
          	declare @valorRemanIni numeric(10,2);
		declare @valorMedio numeric (10,4);
		declare @vlrRemanFinal numeric (10,2);
		declare @dataInicial date;
		declare @finalDate date = '2100-12-31';
		declare @idAeronave int = 1;
		declare @id_trecho int;
		declare @validaAB bit;
		declare @remanFinal numeric (10,2);
		declare @precoMedio numeric (10,4);
		declare @dtTrecho date;
		declare @medida varchar(5);
    
		--AERONAVE
		select @valorMedio = VALOR_MEDIO_LITRO_INICIAL , @valorRemanIni = REMANESCENTE_INICIAL , @dataInicial = DATA_REMANESCENTE, @medida = UNIDADE_REMANESCENTE from TB_AERONAVE 
		where ID_AERONAVE = @idAeronave

		IF @medida = 'G'
		BEGIN
		    SET @valorMedio = ROUND(@valorMedio / 3.7854, 4)
			SET @valorRemanIni = ROUND(CEILING(@valorRemanIni * 3.7854), 2)
		END

		IF @medida = 'LBS'
		BEGIN
		    SET @valorMedio = ROUND(@valorMedio / 0.5617, 4)
			SET @valorRemanIni = ROUND(CEILING(@valorRemanIni * 0.5617), 2)
		END       
          
		--Tabela princial
		create table #principal	(IdReman int identity,
								DtAbastecimento date,
                MoedaAbastecimento int,
								LitrosAbastecidos numeric (10,2),
								ValorLitros numeric(10,4),
								ValorAbastecido numeric(10,2),
								RemanInicial numeric(10,2),
								ValorRemanInicial numeric(10,2),
								PrecoMedio numeric(10,4),
								ConsumoTrecho numeric(10,2),
								ConsumoValor numeric(10,2),
								RemanFinal numeric(10,2),
								ValorRemanFinal numeric(10,2),
								DataTrecho date,
								OrigemDestino varchar(20),
								IdTrecho int,
                Info bit)
		
		insert into #principal(DtAbastecimento,
                MoedaAbastecimento,
								LitrosAbastecidos,
								ValorLitros,
								ValorAbastecido,
								RemanInicial,
								ValorRemanInicial,
								PrecoMedio,
								ConsumoTrecho,
								ConsumoValor ,
								RemanFinal,
								ValorRemanFinal,
								DataTrecho,
								OrigemDestino,
								IdTrecho,
                Info)
									
			SELECT top 1 
			 @dataInicial, 
				1,
			   @valorRemanIni,
			   @valorMedio,
			   round((@valorRemanIni * @valorMedio),2),
			   @valorRemanIni,
			   round((@valorRemanIni * @valorMedio),2),
			   round(@valorMedio,4),
			   0,
			   0,
			   @valorRemanIni,
			   round((@valorRemanIni * @valorMedio),2),
			   @dataInicial,
			   '????-????',
			   0,
			   1
			   FROM TB_TRECHO T 
			   LEFT JOIN TB_ABASTECIMENTO AB ON T.ID_TRECHO = AB.ID_TRECHO
			   inner join TB_AEROPORTO ao on T.ID_AEROPORTO_ORIGEM = ao.ID_AEROPORTO
				inner join TB_AEROPORTO ad on T.ID_AEROPORTO_DESTINO= ad.ID_AEROPORTO
				inner join TB_CIDADE co on ao.ID_CIDADE = co.ID_CIDADE
				inner join TB_CIDADE cd on ad.ID_CIDADE = cd.ID_CIDADE
				where t.ID_AERONAVE = @idAeronave
				AND T.DT_POUSO is not null
				AND T.DT_POUSO BETWEEN (@dataInicial) AND @finalDate 
				order by T.DT_DECOLAGEM


			declare @cur_DtAbastecimento	date;
      			declare @cur_MoedaAbastecimento int;
			declare @cur_LitrosAbastecidos	numeric (10,2);
			declare @cur_ValorAbastecido	numeric(10,2);
			declare @cur_ValorLitros	numeric(10,4);
			declare @cur_ConsumoTrecho	numeric(10,2);
			declare @cur_DataTrecho		date;
			declare @cur_OrigemDestino	varchar(20);
			declare @cur_IdTrecho		int;
      			declare @cur_horaDecolagem      varchar(6);
		    
		    	declare cursor_remanescente cursor for

			select
			DISTINCT AB.DATA,
      			AB.ID_TIPO_MOEDA,
			CASE WHEN ((select COUNT(ID_TRECHO) from TB_ABASTECIMENTO WHERE ID_TRECHO = T.ID_TRECHO) > 0 ) THEN
					SUM(ISNULL((AB.LITROS),0)) / (select COUNT(ID_TRECHO) from TB_ABASTECIMENTO WHERE ID_TRECHO = T.ID_TRECHO)
				ELSE
					SUM(ISNULL((AB.LITROS),0))
				END,
				CASE WHEN ((select COUNT(ID_TRECHO) from TB_ABASTECIMENTO WHERE ID_TRECHO = T.ID_TRECHO) > 0 ) THEN
					SUM(ISNULL((AB.VALOR_LITRO),0)) / (select COUNT(ID_TRECHO) from TB_ABASTECIMENTO WHERE ID_TRECHO = T.ID_TRECHO)
				ELSE
					SUM(ISNULL((AB.VALOR_LITRO),0))
				END,
				CASE WHEN ((select COUNT(ID_TRECHO) from TB_ABASTECIMENTO WHERE ID_TRECHO = T.ID_TRECHO) > 0 ) THEN
					SUM(ISNULL((AB.VALOR),0)) / (select COUNT(ID_TRECHO) from TB_ABASTECIMENTO WHERE ID_TRECHO = T.ID_TRECHO)
				ELSE
					SUM(ISNULL((AB.VALOR),0))
				END, 
			 T.LITROS_CONSUMIDOS,
			 T.DT_POUSO,
			 ISNULL(ao.Sigla,'') + ' - ' + ISNULL(ad.Sigla,'') as OrigemDestino,
			 T.ID_TRECHO,
       			 T.HORA_DECOLAGEM
			 from TB_TRECHO T 
				LEFT JOIN TB_ABASTECIMENTO AB ON T.ID_TRECHO = AB.ID_TRECHO
				inner join TB_AEROPORTO ao on T.ID_AEROPORTO_ORIGEM = ao.ID_AEROPORTO
				inner join TB_AEROPORTO ad on T.ID_AEROPORTO_DESTINO= ad.ID_AEROPORTO
				inner join TB_CIDADE co on ao.ID_CIDADE = co.ID_CIDADE
				inner join TB_CIDADE cd on ad.ID_CIDADE = cd.ID_CIDADE
			where t.ID_AERONAVE = @idAeronave
			AND T.ID_TRECHO not in (select top 1 IdTrecho from #principal)
			AND T.DT_POUSO is not null
			AND T.DT_POUSO BETWEEN (@dataInicial) AND @finalDate
			GROUP BY T.ID_TRECHO, DT_POUSO, AB.DATA, AO.SIGLA, AD.SIGLA, T.LITROS_CONSUMIDOS, AB.ID_TIPO_MOEDA, T.HORA_DECOLAGEM
			order by DT_POUSO, HORA_DECOLAGEM

		open cursor_remanescente 
								
		fetch next from cursor_remanescente into @cur_DtAbastecimento,			
							 @cur_MoedaAbastecimento,
							      @cur_LitrosAbastecidos,
							      @cur_ValorLitros,
							      @cur_ValorAbastecido,
							      @cur_ConsumoTrecho,
							      @cur_DataTrecho,	
							      @cur_OrigemDestino,
							      @cur_IdTrecho,
                                              		      @cur_horaDecolagem
		
		
		--Variáveis iniciais
		set @dtTrecho = (select top 1 DataTrecho from #principal)
		set @remanFinal = ISNULL((SELECT TOP 1 RemanFinal FROM #principal), 0)
		set @precoMedio = ISNULL((SELECT TOP 1 PrecoMedio FROM #principal), 0)
		set @vlrRemanFinal = ISNULL((SELECT TOP 1 ValorRemanFinal FROM #principal), 0)
		set @id_trecho	= ISNULL((SELECT TOP 1 ID_TRECHO FROM TB_TRECHO WHERE ID_TRECHO > (SELECT TOP 1 IdTrecho FROM #principal) AND DT_POUSO >= @dtTrecho AND ID_AERONAVE = @idAeronave order by DT_POUSO, HORA_DECOLAGEM),0)
		set @validaAB = 1;

		create table #trechos (Id int identity, IdTbTrecho int)

		insert into #trechos(IdTbTrecho)
		
		select ID_TRECHO from tb_trecho 
		WHERE 
		DT_POUSO is not null
		order by DT_POUSO, HORA_DECOLAGEM				

		while @@FETCH_STATUS = 0
			begin 
				if(@validaAB = 1)
					begin
						
						  set @id_trecho = (select top 1 IdTbTrecho from #trechos where id > (SELECT top 1 id FROM #trechos where IdTbTrecho = @cur_IdTrecho))
										
						  insert into #principal (DtAbastecimento, MoedaAbastecimento, LitrosAbastecidos, ValorLitros, ValorAbastecido, RemanInicial, ValorRemanInicial, PrecoMedio, ConsumoTrecho, ConsumoValor, RemanFinal, ValorRemanFinal, DataTrecho, OrigemDestino, IdTrecho, Info)
							  select
								    @cur_DtAbastecimento, --Data Abastecimento	
                    							  @cur_MoedaAbastecimento, --Moeda
									  @cur_LitrosAbastecidos, -- Litros Abastecidos
									  @cur_ValorLitros, -- Valor do Litro
									  @cur_ValorAbastecido, --Valor Abastecido
									  @remanFinal + @cur_LitrosAbastecidos, --Remanescente Inicial 
									  (@remanFinal * @precoMedio) + (@cur_LitrosAbastecidos * @cur_ValorLitros), --Valor Remanescente Inicial
									  Round(((@remanFinal * @precoMedio) + (@cur_LitrosAbastecidos * @cur_ValorLitros)) / (@remanFinal + @cur_LitrosAbastecidos),4), --Preço Medio
									  @cur_ConsumoTrecho, -- Consumido no Trecho
									  Round(((@remanFinal * @precoMedio) + (@cur_LitrosAbastecidos * @cur_ValorLitros)) / (@remanFinal + @cur_LitrosAbastecidos),4) * @cur_ConsumoTrecho, -- Consumido Valor
									  (@remanFinal + @cur_LitrosAbastecidos) - @cur_ConsumoTrecho, --Remanescente Final
									  ((@remanFinal * @precoMedio) + (@cur_LitrosAbastecidos * @cur_ValorLitros)) - Round(((@remanFinal * @precoMedio) + (@cur_LitrosAbastecidos * @cur_ValorLitros)) / (@remanFinal + @cur_LitrosAbastecidos),4) * @cur_ConsumoTrecho, --Valor Remanescente final
									  @cur_DataTrecho, -- Data Pouso
									  @cur_OrigemDestino, -- Origem Destino
									  @cur_IdTrecho, -- ID do Trecho
									  0 -- Info
									  set @vlrRemanFinal	= ((@remanFinal * @precoMedio) + (@cur_LitrosAbastecidos * @cur_ValorLitros)) - Round(((@remanFinal * @precoMedio) + (@cur_LitrosAbastecidos * @cur_ValorLitros)) / (@remanFinal + @cur_LitrosAbastecidos),4) * @cur_ConsumoTrecho
									  set @precoMedio		= Round(((@remanFinal * @precoMedio) + (@cur_LitrosAbastecidos * @cur_ValorLitros)) / (@remanFinal + @cur_LitrosAbastecidos),4)
									  set @remanFinal		= @remanFinal + @cur_LitrosAbastecidos - @cur_ConsumoTrecho
									  
									  set @validaAB = ISNULL((select TOP 1 CASE WHEN (ID_ABASTECIMENTO IS null) THEN 0 ELSE 1 END from TB_ABASTECIMENTO where ID_TRECHO = @id_trecho),0)			
				  end
				  	else
				  begin	
          
				  		set @id_trecho = (select top 1 IdTbTrecho from #trechos where id > (SELECT top 1 id FROM #trechos where IdTbTrecho = @cur_IdTrecho))

				  		insert into #principal (DtAbastecimento, MoedaAbastecimento, LitrosAbastecidos, ValorLitros, ValorAbastecido, RemanInicial, ValorRemanInicial, PrecoMedio, ConsumoTrecho, ConsumoValor, RemanFinal, ValorRemanFinal, DataTrecho, OrigemDestino, IdTrecho, Info)
				  		  select
				  				  @cur_DtAbastecimento, --Data Abastecimento	
                    	         				  @cur_MoedaAbastecimento, --Moeda
				  				  @cur_LitrosAbastecidos, -- Litros Abastecidos
				  				  @cur_ValorLitros, -- Valor do Litro
				  				  @cur_ValorAbastecido, --Valor Abastecido
				  				  @remanFinal, --Remanescente Inicial 
				  				  @vlrRemanFinal, --Valor Remanescente Inicial
				  				  @precoMedio, --Preço Medio
				  				  @cur_ConsumoTrecho, -- Consumido no Trecho
				  				  @precoMedio * @cur_ConsumoTrecho, -- Consumido Valor
				  				  @remanFinal - @cur_ConsumoTrecho, --Remanescente Final
				  			     (@remanFinal - @cur_ConsumoTrecho) * @precoMedio, --Valor Remanescente final
				  				  @cur_DataTrecho, -- Data Pouso
				  				  @cur_OrigemDestino, -- Origem Destino
				  				  @cur_IdTrecho, -- ID do Trecho
                    						  0 -- Info
                    
				  				  set @remanFinal = @remanFinal - @cur_ConsumoTrecho
				  				  set @validaAB = ISNULL((select top 1 CASE WHEN (ID_ABASTECIMENTO IS null) THEN 0 ELSE 1 END from TB_ABASTECIMENTO where ID_TRECHO = @id_trecho),0)
				  							
				  end	

				fetch next from cursor_remanescente	into @cur_DtAbastecimento,
                                                  @cur_MoedaAbastecimento,
														                      @cur_LitrosAbastecidos,
														                      @cur_ValorLitros,
														                      @cur_ValorAbastecido,
														                      @cur_ConsumoTrecho,
														                      @cur_DataTrecho,	
														                      @cur_OrigemDestino,
														                      @cur_IdTrecho,
                                                  @cur_horaDecolagem
		end

		close cursor_remanescente
		deallocate cursor_remanescente

		SELECT * FROM #principal where DataTrecho BETWEEN '2000-01-01' AND '2100-01-01' order by DataTrecho			

		drop table #trechos
		drop table #principal
 
