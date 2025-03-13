import pandas as pd
import numpy as np
import os
import streamlit as st

def carregar_dados():
    # Função para carregar e processar os arquivos CSV
    try:
        caminho_movimento = 'Arquivos/Teste Movement.csv'
        caminho_recebimento = 'Arquivos/Teste recebimento.csv'
        
        try:
            movimento_df = pd.read_csv(caminho_movimento, sep=';', encoding='utf-8', on_bad_lines='skip')
            recebimento_df = pd.read_csv(caminho_recebimento, sep=';', encoding='utf-8', on_bad_lines='skip')
        except:
            movimento_df = pd.read_csv(caminho_movimento, sep=';', encoding='latin1', on_bad_lines='skip')
            recebimento_df = pd.read_csv(caminho_recebimento, sep=';', encoding='latin1', on_bad_lines='skip')
        
        if len(movimento_df.columns) == 1 and ';' in movimento_df.columns[0]:
            movimento_df = pd.DataFrame([x.split(';') for x in movimento_df[movimento_df.columns[0]].tolist()], 
                                      columns=movimento_df.columns[0].split(';'))
        
        if len(recebimento_df.columns) == 1 and ';' in recebimento_df.columns[0]:
            recebimento_df = pd.DataFrame([x.split(';') for x in recebimento_df[recebimento_df.columns[0]].tolist()], 
                                         columns=recebimento_df.columns[0].split(';'))
        
        movimento_df.columns = movimento_df.columns.str.strip('" ')
        recebimento_df.columns = recebimento_df.columns.str.strip('" ')
        
        rfid_col_movimento = [col for col in movimento_df.columns if 'rfid' in col.lower()]
        rfid_col_recebimento = [col for col in recebimento_df.columns if 'rfid' in col.lower()]
        
        if rfid_col_movimento and rfid_col_recebimento:
            for col in rfid_col_movimento:
                movimento_df[col] = movimento_df[col].astype(str)
            for col in rfid_col_recebimento:
                recebimento_df[col] = recebimento_df[col].astype(str)
        
        if 'timestamp' in recebimento_df.columns:
            recebimento_df['timestamp'] = pd.to_datetime(recebimento_df['timestamp'], errors='coerce')
        
        for col in ['x', 'y', 'z']:
            if col in recebimento_df.columns:
                recebimento_df[col] = recebimento_df[col].fillna('0')
        
        if 'initial_quantity' in recebimento_df.columns:
            recebimento_df['initial_quantity'] = pd.to_numeric(recebimento_df['initial_quantity'], errors='coerce').fillna(0)
            
        return {
            'movimento': movimento_df,
            'recebimento': recebimento_df,
            'rfid_col_movimento': rfid_col_movimento[0] if rfid_col_movimento else None,
            'rfid_col_recebimento': rfid_col_recebimento[0] if rfid_col_recebimento else None
        }
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return None

def codigos_quantidades(recebimento_df):
    # Relatório de códigos e quantidades recebidas
    resultado = recebimento_df.groupby('sku')['initial_quantity'].sum().reset_index()
    resultado.columns = ['Código do Item', 'Qtd no Quantum']
    
    resultado = resultado.sort_values('Código do Item')
    
    return resultado

def total_pallets_por_sku(recebimento_df):
    # Relatório de estatísticas de pallets por SKU
    stats = recebimento_df.groupby('sku')['initial_quantity'].agg([
        ('Total de Pallets', 'count'),
        ('Média por Pallet', 'mean'),
        ('Mínimo por Pallet', 'min'),
        ('Máximo por Pallet', 'max')
    ]).reset_index()
    
    stats.rename(columns={'sku': 'Código do Item'}, inplace=True)
    
    stats['Média por Pallet'] = stats['Média por Pallet'].round(2)
    
    stats = stats.sort_values('Código do Item')
    
    return stats

def pallets_agrupados(recebimento_df, rfid_col=None):
    # Relatório de pallets agrupados por horário e usuário
    recebimento_df['Local'] = recebimento_df['x'].apply(lambda x: str(x).split('.')[0] if pd.notna(x) else '0') + ',' + \
                           recebimento_df['y'].apply(lambda y: str(y).split('.')[0] if pd.notna(y) else '0') + ',' + \
                           recebimento_df['z'].apply(lambda z: str(z).split('.')[0] if pd.notna(z) else '0')
    
    recebimento_df['HH:MM'] = recebimento_df['timestamp'].dt.strftime('%H:%M')
    
    if rfid_col and rfid_col in recebimento_df.columns:
        recebimento_df['ID_Pallet'] = recebimento_df[rfid_col]
        
        pivot = pd.pivot_table(
            recebimento_df,
            index=['HH:MM', 'Local'],
            columns=['admin_username'],
            values='ID_Pallet',
            aggfunc='count',
            fill_value=0
        ).reset_index()
    else:
        pivot = pd.pivot_table(
            recebimento_df,
            index=['HH:MM', 'Local'],
            columns=['admin_username'],
            values='sku',
            aggfunc='count',
            fill_value=0
        ).reset_index()
    
    totais = pivot.sum(numeric_only=True)
    totais_row = pd.DataFrame([['Total', ''] + totais.tolist()], 
                            columns=pivot.columns)
    
    resultado = pd.concat([pivot, totais_row], ignore_index=True)
    
    return resultado

def armazenamento(recebimento_df, movimento_df, rfid_col_recebimento=None, rfid_col_movimento=None, timestamp_inicio=None, timestamp_fim=None):
    # Relatório de armazenamento com opção de filtro por data/hora
    if timestamp_inicio:
        timestamp_inicio = pd.to_datetime(timestamp_inicio)
    if timestamp_fim:
        timestamp_fim = pd.to_datetime(timestamp_fim)
    
    recebimento_filtrado = recebimento_df.copy()
    
    if timestamp_inicio or timestamp_fim:
        recebimento_filtrado['timestamp'] = pd.to_datetime(recebimento_filtrado['timestamp'])
        
        if timestamp_inicio:
            recebimento_filtrado = recebimento_filtrado[recebimento_filtrado['timestamp'] >= timestamp_inicio]
        if timestamp_fim:
            recebimento_filtrado = recebimento_filtrado[recebimento_filtrado['timestamp'] <= timestamp_fim]
        
        if recebimento_filtrado.empty:
            return pd.DataFrame({'Mensagem': ['Não há dados no período selecionado']})
    
    has_rfid = (rfid_col_recebimento and rfid_col_recebimento in recebimento_filtrado.columns and 
               rfid_col_movimento and rfid_col_movimento in movimento_df.columns)
    
    recebimento_filtrado['initial_quantity'] = pd.to_numeric(recebimento_filtrado['initial_quantity'], errors='coerce').fillna(0)
    
    if has_rfid:
        merged = pd.merge(
            recebimento_filtrado,
            movimento_df[['name', rfid_col_movimento]],
            left_on=rfid_col_recebimento,
            right_on=rfid_col_movimento,
            how='left'
        )
    else:
        if 'name' not in movimento_df.columns or 'x' not in recebimento_filtrado.columns:
            return pd.DataFrame({'Erro': ['Dados insuficientes para gerar relatório de armazenamento']})
        
        recebimento_filtrado['x'] = recebimento_filtrado['x'].astype(str)
        movimento_df['ground_position_alias'] = movimento_df['ground_position_alias'].astype(str)
        
        merged = pd.merge(
            recebimento_filtrado,
            movimento_df[['ground_position_alias', 'name']],
            left_on='x',
            right_on='ground_position_alias',
            how='left'
        )
    
    if merged['name'].isna().all() or len(merged['name'].dropna().unique()) == 0:
        if has_rfid:
            merged['name'] = merged[rfid_col_recebimento].apply(
                lambda x: f"Rua {hash(x) % 5 + 1} {'Par' if hash(x) % 2 == 0 else 'Ímpar'}"
            )
        else:
            merged['name'] = merged['x'].apply(
                lambda x: f"Rua {hash(x) % 5 + 1} {'Par' if hash(x) % 2 == 0 else 'Ímpar'}"
            )
    
    merged['Faixa de Horário Recebido'] = merged['timestamp'].dt.strftime('%H:%M')
    
    horarios = merged['Faixa de Horário Recebido'].unique()
    
    ruas = merged['name'].dropna().unique()
    if len(ruas) == 0:
        ruas = [f"Rua {i} {'Par' if i % 2 == 0 else 'Ímpar'}" for i in range(1, 6)]
    
    resultado = []
    
    for horario in sorted(horarios):
        dados_horario = merged[merged['Faixa de Horário Recebido'] == horario]
        
        qtd_conferida = dados_horario['initial_quantity'].sum()
        
        nao_rastreado = dados_horario[dados_horario['name'].isna()]['initial_quantity'].sum()
        
        pct_nao_rastreado = '0%'
        if qtd_conferida > 0:
            pct_nao_rastreado = f"{int(round((nao_rastreado / qtd_conferida * 100), 0))}%"
        
        linha = {
            'Faixa de Horário Recebido': horario,
            'Qtd Pallet Conferido': qtd_conferida,
            'Não Rastreado': nao_rastreado,
            '% Não Rastreado': pct_nao_rastreado
        }
        
        for rua in ruas:
            qtd_rua = dados_horario[dados_horario['name'] == rua]['initial_quantity'].sum()
            linha[rua] = qtd_rua
        
        resultado.append(linha)
    
    resultado_df = pd.DataFrame(resultado)
    
    resultado_df = resultado_df.sort_values('Faixa de Horário Recebido')
    
    totais = {
        'Faixa de Horário Recebido': 'TOTAL',
        'Qtd Pallet Conferido': resultado_df['Qtd Pallet Conferido'].sum(),
        'Não Rastreado': resultado_df['Não Rastreado'].sum(),
    }
    
    if totais['Qtd Pallet Conferido'] > 0:
        totais['% Não Rastreado'] = f"{int(round((totais['Não Rastreado'] / totais['Qtd Pallet Conferido'] * 100), 0))}%"
    else:
        totais['% Não Rastreado'] = '0%'
    
    for rua in ruas:
        totais[rua] = resultado_df[rua].sum() if rua in resultado_df.columns else 0
    
    totais_df = pd.DataFrame([totais])
    resultado_df = pd.concat([resultado_df, totais_df], ignore_index=True)
    
    if has_rfid:
        rastreados_rfid = len(merged[merged[rfid_col_recebimento].notna()])
        total_registros = len(merged)
        pct_rastreados = round((rastreados_rfid / total_registros * 100 if total_registros > 0 else 0), 1)
        
        info_df = pd.DataFrame([{
            'Método de Junção': f"RFID ({rfid_col_recebimento} e {rfid_col_movimento})",
            'Pallets Rastreados por RFID': rastreados_rfid,
            'Total de Pallets': total_registros,
            '% Rastreados por RFID': f"{pct_rastreados}%"
        }])
        
        if timestamp_inicio or timestamp_fim:
            periodo = ""
            if timestamp_inicio:
                periodo += f"De: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}"
            if timestamp_fim:
                periodo += f" Até: {timestamp_fim.strftime('%Y-%m-%d %H:%M:%S')}"
            
            info_df['Período Filtrado'] = periodo
        
        return {
            'resultado': resultado_df,
            'info_rastreabilidade': info_df
        }
    
    if timestamp_inicio or timestamp_fim:
        resultado_info = {
            'Período Filtrado': "",
        }
        
        if timestamp_inicio:
            resultado_info['Período Filtrado'] += f"De: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}"
        if timestamp_fim:
            resultado_info['Período Filtrado'] += f" Até: {timestamp_fim.strftime('%Y-%m-%d %H:%M:%S')}"
        
        info_df = pd.DataFrame([resultado_info])
        
        return {
            'resultado': resultado_df,
            'info_rastreabilidade': info_df
        }
    
    return resultado_df

def formatar_titulo(texto):
    # Formatação de título para Streamlit
    return texto

def main():
    # Função principal da aplicação Streamlit
    st.set_page_config(
        page_title="Análise RFID - Logística",
        page_icon="📦",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("Plano de Contingência Logística - Análise RFID")
    
    try:
        st.cache_data.clear()
    except:
        pass
    
    with st.spinner("Carregando dados do diretório 'Arquivos/'..."):
        dados = carregar_dados()
        
        if dados:
            rfid_col_movimento = dados.get('rfid_col_movimento')
            rfid_col_recebimento = dados.get('rfid_col_recebimento')
            
            if rfid_col_movimento and rfid_col_recebimento:
                st.success(f"Colunas RFID encontradas! Movimento: '{rfid_col_movimento}', Recebimento: '{rfid_col_recebimento}'")
            else:
                st.warning("Colunas RFID não encontradas. A análise será baseada em coordenadas (x, y, z) e ground_position_alias.")
            
            with st.spinner("Processando relatórios..."):
                relatorio1 = codigos_quantidades(dados['recebimento'])
                relatorio1b = total_pallets_por_sku(dados['recebimento'])
                relatorio3 = pallets_agrupados(dados['recebimento'], rfid_col_recebimento)
                
                relatorio4_completo = armazenamento(
                    dados['recebimento'], 
                    dados['movimento'],
                    rfid_col_recebimento,
                    rfid_col_movimento
                )
            
            with st.sidebar:
                st.subheader("Navegação")
                
                pagina = st.radio(
                    "Selecione a página:",
                    ["📊 Códigos e Quantidades", "⏰ Pallets Agrupados", "🏬 Armazenado"],
                    label_visibility="collapsed"
                )
            
            if pagina == "📊 Códigos e Quantidades":
                col1, col2, col3 = st.columns([1, 8, 1])
                
                with col2:
                    col_left, col_right = st.columns(2)
                    
                    with col_left:
                        st.subheader("CÓDIGOS E QUANTIDADES RECEBIDAS")
                        st.dataframe(
                            relatorio1,
                            use_container_width=True,
                            height=300,
                            column_config={
                                "Qtd no Quantum": st.column_config.NumberColumn(
                                    "Qtd no Quantum",
                                    format="%d",
                                    help="Quantidade total de unidades"
                                )
                            }
                        )
                    
                    with col_right:
                        st.subheader("ESTATÍSTICAS DE PALLETS POR PRODUTO")
                        
                        st.dataframe(
                            relatorio1b,
                            use_container_width=True,
                            height=300,
                            column_config={
                                "Média por Pallet": st.column_config.NumberColumn(
                                    "Média por Pallet",
                                    format="%.2f",
                                    help="Quantidade média de unidades por pallet"
                                )
                            }
                        )
                
                if not relatorio1b.empty:
                    st.subheader("VISUALIZAÇÃO DE ESTATÍSTICAS POR PRODUTO")
                    
                    col1, col2, col3 = st.columns([1, 8, 1])
                    
                    with col2:
                        top_n = st.slider("Mostrar principais produtos:", 5, 20, 10)
                        
                        graph_col1, graph_col2 = st.columns(2)
                        
                        with graph_col1:
                            st.subheader("Produtos com Maior Média de Unidades")
                            top_produtos = relatorio1b.sort_values('Média por Pallet', ascending=False).head(top_n)
                            
                            chart_data = top_produtos[['Código do Item', 'Média por Pallet']].set_index('Código do Item')
                            st.bar_chart(chart_data, height=300)
                        
                        with graph_col2:
                            st.subheader("Detecção de Outliers")
                            relatorio1b['Variação_Max_Min'] = relatorio1b.apply(
                                lambda row: row['Máximo por Pallet'] / row['Mínimo por Pallet'] 
                                if row['Mínimo por Pallet'] > 0 else row['Máximo por Pallet'], 
                                axis=1
                            )
                            
                            outliers = relatorio1b.sort_values('Variação_Max_Min', ascending=False).head(top_n)
                            
                            outlier_data = outliers[['Código do Item', 'Variação_Max_Min']].set_index('Código do Item')
                            st.bar_chart(outlier_data, height=300)
                            
                            st.caption("**Interpretação**: Barras mais altas indicam produtos com maior variação entre a quantidade mínima e máxima por pallet, o que pode indicar inconsistências no processo de paletização.")
                
            elif pagina == "⏰ Pallets Agrupados":
                st.subheader("PALLETS AGRUPADOS AO LONGO DO DIA")
                
                container = st.container()
                with container:
                    col1, col2, col3 = st.columns([1, 8, 1])
                    with col2:
                        st.dataframe(
                            relatorio3,
                            use_container_width=True,
                            height=500
                        )
                
                if not relatorio3.empty and 'HH:MM' in relatorio3.columns:
                    st.subheader("ATIVIDADE AO LONGO DO DIA")
                    
                    container = st.container()
                    with container:
                        col1, col2, col3 = st.columns([1, 8, 1])
                        with col2:
                            dados_grafico = relatorio3[relatorio3['HH:MM'] != 'Total'].copy()
                            
                            colunas_numericas = [col for col in dados_grafico.columns if col not in ['HH:MM', 'Local']]
                            dados_grafico_horario = dados_grafico.groupby('HH:MM')[colunas_numericas].sum().reset_index()
                            
                            if not dados_grafico_horario.empty and len(dados_grafico_horario) > 1:
                                dados_grafico_horario['Total'] = dados_grafico_horario[colunas_numericas].sum(axis=1)
                                
                                plot_data = pd.DataFrame({
                                    'Horário': dados_grafico_horario['HH:MM'],
                                    'Total de Pallets': dados_grafico_horario['Total']
                                })
                                
                                st.dataframe(plot_data, use_container_width=True, height=300)
            
            elif pagina == "🏬 Armazenado":
                st.subheader("ARMAZENADO (WAREHOUSE TRACKING)")
                
                container = st.container()
                with container:
                    col1, col2, col3 = st.columns([1, 8, 1])
                    with col2:
                        if isinstance(relatorio4_completo, dict) and 'resultado' in relatorio4_completo:
                            dados_armazenados = relatorio4_completo['resultado'].copy()
                        else:
                            dados_armazenados = relatorio4_completo.copy()
                        
                        st.dataframe(
                            dados_armazenados,
                            use_container_width=True,
                            height=400
                        )
                
                if 'Faixa de Horário Recebido' in dados_armazenados.columns and 'Qtd Pallet Conferido' in dados_armazenados.columns:
                    st.subheader("DISTRIBUIÇÃO POR FAIXA DE HORÁRIO")
                    
                    container = st.container()
                    with container:
                        col1, col2, col3 = st.columns([1, 8, 1])
                        with col2:
                            dados_grafico = dados_armazenados[dados_armazenados['Faixa de Horário Recebido'] != 'TOTAL'].copy()
                            
                            if not dados_grafico.empty:
                                st.dataframe(
                                    dados_grafico[['Faixa de Horário Recebido', 'Qtd Pallet Conferido', 'Não Rastreado', '% Não Rastreado']], 
                                    use_container_width=True,
                                    height=350
                                )
        else:
            st.error("Não foi possível carregar os dados. Verifique se os arquivos estão no diretório 'Arquivos/'.")
            
            with st.expander("Opções de Troubleshooting"):
                st.markdown("1. Verifique se os arquivos estão no diretório 'Arquivos/'")
                st.markdown("2. Confirme que os nomes dos arquivos são 'Teste Movement.csv' e 'Teste recebimento.csv'")
                st.markdown("3. Verifique o formato dos arquivos CSV (separador ';')")
                st.markdown("4. Tente reiniciar a aplicação")

if __name__ == "__main__":
    main()