#!/usr/bin/env Rscript

# Script para hacer el calculo de scores de ...
# Ejecutar: Rscript ctr_score_flow.r <archive> <path> <prodDB>> log 2> err.log &
# archive format YYYMMDD, ie 20190311
# path: will come from python executor as a parameter
# prodDB will come as parameter to feed db_indi variable. Possible values: 'prod_dev' or 'ctr_comp_prod. DB to read ctr_comp_indicadores attributes.
# limit (for testing): true or false - Not in use yet

# Script para cálculo de flows y scores
rm(list = ls())

# Librerías/Paquetes requeridos
require(DBI)
require(rJava)
require(RJDBC)
require(dplyr)

printf <- function(...) cat(sprintf(...))

start_time <- Sys.time()       #Measuring the time
#archive <- format(as.Date(Sys.Date(), format = '%Y%m%d'), "%Y%m%d")

# Reading archive from parameters
args <- commandArgs(TRUE)
archive <- args[1]
path <- args[2]

#path='/export/home/user/R/score_flow_plus'
file_name='score_flow_plus'


# Parámetros ----
# Columnas índice
idx_columns <- c('infocheck_id', 'informconf_id', 'pers_id', 'tipo_doc_id')
# Base de datos y nombre de la tabla de indicadores
#db_indi <- 'ctr_comp_prod'
db_indi <- args[3]
tb_indi <- 'ctr_comp_indicadores'

# Base de datos y nombre de la tabla resultados
db_resu <- ''
tb_resu <- ''

#DEBUG o Testing - Comentar esta parte
#db_indi <- 'prod_dev'
#path='/export/home/user/R/score_flow_plus'
#archive <- format(as.Date(Sys.Date(), format = '%Y%m%d'), "%Y%m%d")

printf("# ------------------------------------ CALCULO SCORE flow (+) %s------------------------------------- #\n",archive)

# Listado de variables ---
# variables para hardcodear la flow X (score -1)
variables.flow.x <- c(
  'inf_conv_cant_abiertas','inf_dem_cant_total','inf_dem_cant_total_cable',
  'inf_dem_cant_total_ccredito','inf_dem_cant_total_comercio',
  'inf_dem_cant_total_sf','inf_dem_cant_total_spub','inf_dem_cant_total_telco',
  'inf_inc_cant_abiertas','inf_inhib_cant_abiertas','inf_mor_cant_total',
  'inf_mor_cant_total_cable','inf_mor_cant_total_ccredito',
  'inf_mor_cant_total_sf','inf_mor_cant_total_spub','inf_mor_cant_total_telco',
  'inf_qui_cant_abiertas','inf_rem_cant_abiertas')

# variables necesarias para el cálculo de la flow Positiva (Scoreflow+)
variables.flow.pos <- c(
  'inf_conv_cant_abiertas','inf_dem_cant_total','inf_dem_cant_total_cable',
  'inf_dem_cant_total_ccredito','inf_dem_cant_total_comercio',
  'inf_dem_cant_total_sf','inf_dem_cant_total_spub','inf_dem_cant_total_telco',
  'inf_inc_cant_abiertas','inf_inhib_cant_abiertas','inf_mor_cant_total',
  'inf_mor_cant_total_cable','inf_mor_cant_total_ccredito',
  'inf_mor_cant_total_sf','inf_mor_cant_total_spub','inf_mor_cant_total_telco',
  'inf_qui_cant_abiertas','inf_rem_cant_abiertas','scorecard',
  'ich_ch_re_cant_i_fond','ich_cons_cant_total','ich_cons_cant_banco_admin',
  'distrito','edad','estado_civil','ich_fecha_ult_cons_priv',
  'ips_flag_actividad','max_flow','g_max_venc_ten_36','inf_cant_dist_afi_u6m',
  'inf_cant_dist_afi_ccred_u3m','inf_cant_dist_afi_telco_u12m',
  'inf_cons_cant_telco_u1m','inf_cons_cant_total_telco','inf_cons_cant_u1m',
  'inf_cons_ult_dias_ccredito','inf_cons_ult_dias_sf',
  'inf_morce_cant_31_45_abiertas','inf_morce_cant_46_60_abiertas',
  'inf_morce_cant_61_90_abiertas','inf_morce_cant_abiertas',
  'inf_qui_cant_abiertas','p_cant_atraso','p_saldo_vencido_ult_act',
  'p_sdot_monto_gt75_m1','situacion_bancaria','tc_close_total',
  'tc_max_dias_atraso_u6m','tc_primera_act','tc_prom_pmin_limcr_m1',
  'tc_svenc_sdot_gt0_u12m', 'fecha_obs')

# Funciones de cálculo de flows ----
# calculo de flow X por regla de negocio
calcular.flow.x <- function(df){
  x <-
    case_when(
      coalesce(df$inf_mor_cant_total_sf, 0) >= 1 |
        coalesce(df$inf_mor_cant_total_ccredito, 0) >= 1 |
        coalesce(df$inf_dem_cant_total, 0) >= 3 |
        coalesce(df$inf_dem_cant_total_sf, 0) >= 1 |
        coalesce(df$inf_dem_cant_total_comercio, 0) >= 1 |
        coalesce(df$inf_dem_cant_total_ccredito, 0) >= 1 |
        coalesce(df$inf_inhib_cant_abiertas, 0) >= 1 |
        coalesce(df$inf_inc_cant_abiertas, 0) >= 1 |
        coalesce(df$inf_rem_cant_abiertas, 0) >= 1 |
        coalesce(df$inf_conv_cant_abiertas, 0) >= 1 |
        coalesce(df$inf_qui_cant_abiertas, 0) >= 1 |
        coalesce(df$inf_mor_cant_total, 0) >= 3 |
        
        (
          coalesce(df$inf_mor_cant_total, 0)  -
            coalesce(df$inf_mor_cant_total_telco, 0)  -
            coalesce(df$inf_mor_cant_total_cable, 0)  -
            coalesce(df$inf_mor_cant_total_spub, 0)  >= 2
        ) |
        
        (
          coalesce(df$inf_dem_cant_total, 0) -
            coalesce(df$inf_dem_cant_total_telco, 0) -
            coalesce(df$inf_dem_cant_total_cable, 0) -
            coalesce(df$inf_dem_cant_total_spub, 0) >= 2
        ) |
        
        (
          coalesce(df$inf_mor_cant_total, 0) +
            coalesce(df$inf_dem_cant_total, 0) -
            coalesce(df$inf_mor_cant_total_telco, 0) -
            coalesce(df$inf_mor_cant_total_cable, 0) -
            coalesce(df$inf_mor_cant_total_spub, 0) -
            coalesce(df$inf_dem_cant_total_telco, 0) -
            coalesce(df$inf_dem_cant_total_cable, 0) -
            coalesce(df$inf_dem_cant_total_spub, 0) >= 2
        ) |
        
        (
          coalesce(df$inf_mor_cant_total, 0) +
            coalesce(df$inf_dem_cant_total, 0) >= 3
        ) ~ 1,
      
      TRUE ~ 0
    )
  
  return(x)
}

calcular.flow.pos <- function(df, idx_columns = NULL) {
  # Calculo de flow x
  df <- df %>% mutate(flow_x = calcular.flow.x(.))
  
  # fixes de tipos de datos
  df <- df %>%
    mutate(
      # fix porque la tabla está con '0' por predeterminado
      ich_fecha_ult_cons_priv = na_if(ich_fecha_ult_cons_priv, '0'),
      tc_max_dias_atraso_u6m = na_if(tc_max_dias_atraso_u6m, 'N/A'),
      tc_max_dias_atraso_u6m = as.numeric(tc_max_dias_atraso_u6m),
    )
  
  # Flags y nuevas variables
  df <- df %>%
    mutate(
      flag_mnx =
        case_when(coalesce(max_flow, '') %in% c('M', 'N', 'X') ~ 1, TRUE ~ 0),
      
      fecha_ult_cons_privdia =
        case_when(
          is.na(ich_fecha_ult_cons_priv) ~ 115974872,
          TRUE ~ as.numeric(as.POSIXct(fecha_obs) -
                              as.POSIXct(ich_fecha_ult_cons_priv))
        )
    )
  
  # Tratamiento de missings
  df <- df %>%
    mutate(
      scorecard = coalesce(scorecard, 0),
      
      ich_ch_re_cant_i_fond = coalesce(ich_ch_re_cant_i_fond, 0),
      ich_cons_cant_total = coalesce(ich_cons_cant_total, 11),
      ich_cons_cant_banco_admin =
        case_when(
          scorecard == 1 ~ coalesce(ich_cons_cant_banco_admin, 0),
          TRUE ~ coalesce(ich_cons_cant_banco_admin, 2)
        ),
      distrito =
        case_when(
          scorecard == 1 ~ coalesce(distrito, 'HERNANDARIAS'),
          TRUE ~ coalesce(distrito, 'MBOCAYATY DEL GUAIRA')
        ),
      edad = coalesce(edad, 37),
      estado_civil = coalesce(estado_civil, 7),
      ips_flag_actividad = coalesce(ips_flag_actividad, 0),
      g_max_venc_ten_36 = coalesce(g_max_venc_ten_36, 0),
      inf_cant_dist_afi_ccred_u3m = coalesce(inf_cant_dist_afi_ccred_u3m, 5),
      inf_cant_dist_afi_telco_u12m = coalesce(inf_cant_dist_afi_telco_u12m, 0),
      inf_cant_dist_afi_u6m = coalesce(inf_cant_dist_afi_u6m, 0),
      inf_cons_cant_telco_u1m = coalesce(inf_cons_cant_telco_u1m, 0),
      inf_cons_cant_total_telco =  coalesce(inf_cons_cant_total_telco, 0),
      inf_cons_cant_u1m = coalesce(inf_cons_cant_u1m, 0),
      inf_cons_ult_dias_ccredito =
        case_when(
          scorecard == 1 ~ coalesce(inf_cons_ult_dias_ccredito, 833),
          TRUE ~ coalesce(inf_cons_ult_dias_ccredito, 848)
        ),
      inf_cons_ult_dias_sf =
        case_when(
          scorecard == 1 ~ coalesce(inf_cons_ult_dias_sf, 21),
          TRUE ~ coalesce(inf_cons_ult_dias_sf, 162)
        ),
      inf_conv_cant_abiertas = coalesce(inf_conv_cant_abiertas, 0),
      inf_dem_cant_total = coalesce(inf_dem_cant_total, 0),
      inf_inc_cant_abiertas = coalesce(inf_inc_cant_abiertas, 0),
      inf_inhib_cant_abiertas = coalesce(inf_inhib_cant_abiertas, 0),
      inf_morce_cant_31_45_abiertas =
        coalesce(inf_morce_cant_31_45_abiertas, 0),
      inf_morce_cant_46_60_abiertas =
        coalesce(inf_morce_cant_46_60_abiertas, 0),
      inf_morce_cant_61_90_abiertas =
        coalesce(inf_morce_cant_61_90_abiertas, 0),
      inf_morce_cant_abiertas = coalesce(inf_morce_cant_abiertas, 0),
      inf_qui_cant_abiertas = coalesce(inf_qui_cant_abiertas, 0),
      inf_rem_cant_abiertas = coalesce(inf_rem_cant_abiertas, 0),
      p_cant_atraso = coalesce(p_cant_atraso, 0),
      p_saldo_vencido_ult_act = coalesce(p_saldo_vencido_ult_act, 0),
      p_sdot_monto_gt75_m1 = coalesce(p_sdot_monto_gt75_m1, 0),
      situacion_bancaria = coalesce(situacion_bancaria, 2),
      tc_close_total = coalesce(tc_close_total, 5),
      tc_max_dias_atraso_u6m = coalesce(tc_max_dias_atraso_u6m, 2),
      tc_primera_act = coalesce(tc_primera_act, 60),
      tc_prom_pmin_limcr_m1 = coalesce(tc_prom_pmin_limcr_m1, 0),
      tc_svenc_sdot_gt0_u12m = coalesce(tc_svenc_sdot_gt0_u12m, 0)
    )
  
  # Capeos
  df <- df %>%
    mutate(
      ich_ch_re_cant_i_fond =
        case_when(
          ich_ch_re_cant_i_fond < 0 ~ 0,
          ich_ch_re_cant_i_fond > 1 ~ 1,
          TRUE ~ ich_ch_re_cant_i_fond
        ),
      ich_cons_cant_total =
        case_when(
          ich_cons_cant_total < 0 ~ 0,
          ich_cons_cant_total > 55 ~ 55,
          TRUE ~ ich_cons_cant_total
        ),
      ich_cons_cant_banco_admin =
        case_when(
          ich_cons_cant_banco_admin < 0 ~ 0,
          scorecard == 1 & ich_cons_cant_banco_admin > 1 ~ 1,
          scorecard == 0 & ich_cons_cant_banco_admin > 3 ~ 3,
          TRUE ~ ich_cons_cant_banco_admin
        ),
      edad =
        case_when(
          edad < 18 ~ 18,
          scorecard == 1 & edad > 72 ~ 72,
          scorecard == 0 & edad > 78 ~ 78,
          TRUE ~ edad
        ),
      estado_civil =
        case_when(
          estado_civil < 0 ~ 0,
          estado_civil < 7 ~ 7,
          TRUE ~ estado_civil
        ),
      ips_flag_actividad =
        case_when(
          ips_flag_actividad < 0 ~ 0,
          ips_flag_actividad > 1 ~ 1,
          TRUE ~ ips_flag_actividad
        ),
      g_max_venc_ten_36 =
        case_when(
          g_max_venc_ten_36 < 0 ~ 0,
          g_max_venc_ten_36 > 1 ~ 1,
          TRUE ~ g_max_venc_ten_36
        ),
      inf_cant_dist_afi_ccred_u3m =
        case_when(
          inf_cant_dist_afi_ccred_u3m < 0 ~ 0,
          inf_cant_dist_afi_ccred_u3m > 5 ~ 5,
          TRUE ~ inf_cant_dist_afi_ccred_u3m
        ),
      inf_cant_dist_afi_telco_u12m =
        case_when(
          inf_cant_dist_afi_telco_u12m < 0 ~ 0,
          inf_cant_dist_afi_telco_u12m > 2 ~ 2,
          TRUE ~ inf_cant_dist_afi_telco_u12m
        ),
      inf_cant_dist_afi_u6m =
        case_when(
          inf_cant_dist_afi_u6m < 0 ~ 0,
          scorecard == 1 & inf_cant_dist_afi_u6m > 21 ~ 21,
          scorecard == 0 & inf_cant_dist_afi_u6m > 14 ~ 14,
          TRUE ~ inf_cant_dist_afi_u6m
        ),
      inf_cons_cant_telco_u1m =
        case_when(
          inf_cons_cant_telco_u1m < 0 ~ 0,
          inf_cons_cant_telco_u1m > 1 ~ 1,
          TRUE ~ inf_cons_cant_telco_u1m
        ),
      inf_cons_cant_total_telco =
        case_when(
          inf_cons_cant_total_telco < 0 ~ 0,
          inf_cons_cant_total_telco > 7 ~ 7,
          TRUE ~ inf_cons_cant_total_telco
        ),
      inf_cons_cant_u1m =
        case_when(
          inf_cons_cant_u1m < 0 ~ 0,
          inf_cons_cant_u1m > 6 ~ 6,
          TRUE ~ inf_cons_cant_u1m
        ),
      inf_cons_ult_dias_ccredito =
        case_when(
          inf_cons_ult_dias_ccredito < 0 ~ 0,
          scorecard == 1 & inf_cons_ult_dias_ccredito > 975 ~ 975,
          scorecard == 0 & inf_cons_ult_dias_ccredito > 1036 ~ 1036,
          TRUE ~ inf_cons_ult_dias_ccredito
        ),
      inf_cons_ult_dias_sf =
        case_when(
          inf_cons_ult_dias_sf < 0 ~ 0,
          scorecard == 1 & inf_cons_ult_dias_sf > 736 ~ 736,
          scorecard == 0 & inf_cons_ult_dias_sf > 987 ~ 987,
          TRUE ~ inf_cons_ult_dias_sf
        ),
      inf_conv_cant_abiertas =
        case_when(
          inf_conv_cant_abiertas < 0 ~ 0,
          inf_conv_cant_abiertas > 1 ~ 1,
          TRUE ~ inf_conv_cant_abiertas
        ),
      inf_dem_cant_total =
        case_when(
          inf_dem_cant_total < 0 ~ 0,
          inf_dem_cant_total > 1 ~ 1,
          TRUE ~ inf_dem_cant_total
        ),
      inf_inc_cant_abiertas =
        case_when(
          inf_inc_cant_abiertas < 0 ~ 0,
          inf_inc_cant_abiertas > 1 ~ 1,
          TRUE ~ inf_inc_cant_abiertas
        ),
      inf_inhib_cant_abiertas =
        case_when(
          inf_inhib_cant_abiertas < 0 ~ 0,
          inf_inhib_cant_abiertas > 1 ~ 1,
          TRUE ~ inf_inhib_cant_abiertas
        ),
      inf_morce_cant_31_45_abiertas =
        case_when(
          inf_morce_cant_31_45_abiertas < 0 ~ 0,
          inf_morce_cant_31_45_abiertas > 2 ~ 2,
          TRUE ~ inf_morce_cant_31_45_abiertas
        ),
      inf_morce_cant_46_60_abiertas =
        case_when(
          inf_morce_cant_46_60_abiertas < 0 ~ 0,
          inf_morce_cant_46_60_abiertas > 1 ~ 1,
          TRUE ~ inf_morce_cant_46_60_abiertas
        ),
      inf_morce_cant_61_90_abiertas =
        case_when(
          inf_morce_cant_61_90_abiertas < 0 ~ 0,
          inf_morce_cant_61_90_abiertas > 1 ~ 1,
          TRUE ~ inf_morce_cant_61_90_abiertas
        ),
      inf_morce_cant_abiertas =
        case_when(
          inf_morce_cant_abiertas < 0 ~ 0,
          inf_morce_cant_abiertas > 1 ~ 1,
          TRUE ~ inf_morce_cant_abiertas
        ),
      inf_qui_cant_abiertas =
        case_when(
          inf_qui_cant_abiertas < 0 ~ 0,
          inf_qui_cant_abiertas > 1 ~ 1,
          TRUE ~ inf_qui_cant_abiertas
        ),
      inf_rem_cant_abiertas =
        case_when(
          inf_rem_cant_abiertas < 0 ~ 0,
          inf_rem_cant_abiertas > 1 ~ 1,
          TRUE ~ inf_rem_cant_abiertas
        ),
      p_cant_atraso =
        case_when(
          p_cant_atraso < 0 ~ 0,
          p_cant_atraso > 5 ~ 5,
          TRUE ~ p_cant_atraso
        ),
      p_saldo_vencido_ult_act =
        case_when(
          p_saldo_vencido_ult_act < 0 ~ 0,
          p_saldo_vencido_ult_act > 6372816 ~ 6372816,
          TRUE ~ p_saldo_vencido_ult_act
        ),
      p_sdot_monto_gt75_m1 =
        case_when(
          p_sdot_monto_gt75_m1 < 0 ~ 0,
          p_sdot_monto_gt75_m1 > 2 ~ 2,
          TRUE ~ p_sdot_monto_gt75_m1
        ),
      situacion_bancaria =
        case_when(
          situacion_bancaria < 1 ~ 1,
          situacion_bancaria > 2 ~ 2,
          TRUE ~ situacion_bancaria
        ),
      tc_close_total =
        case_when(
          tc_close_total < 0 ~ 0,
          tc_close_total > 7 ~ 7,
          TRUE ~ tc_close_total
        ),
      tc_max_dias_atraso_u6m =
        case_when(
          tc_max_dias_atraso_u6m < 0 ~ 0,
          tc_max_dias_atraso_u6m > 85 ~ 85,
          TRUE ~ tc_max_dias_atraso_u6m
        ),
      tc_primera_act =
        case_when(
          tc_primera_act < 0 ~ 0,
          tc_primera_act > 63 ~ 63,
          TRUE ~ tc_primera_act
        ),
      tc_prom_pmin_limcr_m1 =
        case_when(
          tc_prom_pmin_limcr_m1 < 0 ~ 0,
          tc_prom_pmin_limcr_m1 > 0.3715 ~ 0.3715,
          TRUE ~ tc_prom_pmin_limcr_m1
        ),
      tc_svenc_sdot_gt0_u12m =
        case_when(
          tc_svenc_sdot_gt0_u12m < 0 ~ 0,
          tc_svenc_sdot_gt0_u12m > 2 ~ 2,
          TRUE ~ tc_svenc_sdot_gt0_u12m
        )
    )
  
  # Transformación de variables
  df <- df %>%
    mutate(
      casado = case_when(estado_civil == 2 ~ 1, TRUE ~ 0),
      flag_judiciales =
        case_when(
          coalesce(inf_dem_cant_total, 0) > 0        |
            coalesce(inf_conv_cant_abiertas, 0) > 0  |
            coalesce(inf_qui_cant_abiertas, 0) > 0   |
            coalesce(inf_rem_cant_abiertas, 0) > 0   |
            coalesce(inf_inhib_cant_abiertas, 0) > 0 |
            coalesce(inf_inc_cant_abiertas, 0) > 0   ~ 1,
          TRUE ~ 0
        ),
      flag_saldo_vencido_ult_gt0 =
        case_when(p_saldo_vencido_ult_act > 0 ~ 1, TRUE ~ 0),
      situacion_bancaria_woe =
        case_when(
          situacion_bancaria == 1 ~    2.035253892,
          situacion_bancaria == 2 ~ -173.2934237,
          TRUE ~ 0
        ),
      ich_cons_cant_total_recod =
        case_when(ich_cons_cant_total == 0 ~ 3, TRUE ~ ich_cons_cant_total),
      distrito_woe =
        case_when(
          scorecard == 1 ~
            case_when(
              distrito == "1RO. DE MARZO"               ~ 0.393333333333,
              distrito == "25 DE DICIEMBRE"             ~ 0.297900262467,
              distrito == "3 DE FEBRERO"                ~ 0.348837209302,
              distrito == "3 DE MAYO"                   ~ 0.335526315789,
              distrito == "ABAI"                        ~ 0.273188405797,
              distrito == "ACAHAY"                      ~ 0.379634753735,
              distrito == "ALBERDI"                     ~ 0.235474006116,
              distrito == "ALTO VERA"                   ~ 0.388446215139,
              distrito == "ALTOS"                       ~ 0.409556313993,
              distrito == "ANTEQUERA"                   ~ 0.431179775281,
              distrito == "AREGUA"                      ~ 0.398593634345,
              distrito == "ARGENTINA"                   ~ 0.352941176471,
              distrito == "ARROYOS Y ESTEROS"           ~ 0.455000000000,
              distrito == "ASUNCION"                    ~ 0.326369429989,
              distrito == "ATYRA"                       ~ 0.503743315508,
              distrito == "AYOLAS"                      ~ 0.512553582364,
              distrito == "AZOTEY"                      ~ 0.368705035971,
              distrito == "BAHIA NEGRA"                 ~ 0.561403508772,
              distrito == "BELEN"                       ~ 0.429099876695,
              distrito == "BELLA VISTA"                 ~ 0.257665411512,
              distrito == "BENJAMIN ACEVAL"             ~ 0.393538913363,
              distrito == "BORJA"                       ~ 0.309392265193,
              distrito == "BUENA VISTA"                 ~ 0.233663366337,
              distrito == "CAACUPE"                     ~ 0.455610021786,
              distrito == "CAAGUAZU"                    ~ 0.364451417685,
              distrito == "CAAPUCU"                     ~ 0.360747663551,
              distrito == "CAAZAPA"                     ~ 0.366233766234,
              distrito == "CAMBYRETA"                   ~ 0.305675406572,
              distrito == "CAP. MAURICIO JOSE TROCHE"   ~ 0.257477243173,
              distrito == "CAPIATA"                     ~ 0.396758081236,
              distrito == "CAPIIVARY"                   ~ 0.333485818847,
              distrito == "CAPITAN BADO"                ~ 0.316639741519,
              distrito == "CAPITAN MEZA"                ~ 0.351087771943,
              distrito == "CAPITAN MIRANDA"             ~ 0.349336057201,
              distrito == "CARAGUATAY"                  ~ 0.328836424958,
              distrito == "CARAPEGUA"                   ~ 0.268552685527,
              distrito == "CARAYAO"                     ~ 0.454838709677,
              distrito == "CARLOS ANTONIO LOPEZ"        ~ 0.304575163399,
              distrito == "CARMELO PERALTA"             ~ 0.329545454545,
              distrito == "CARMEN DEL PARANA"           ~ 0.311231393775,
              distrito == "CERRITO"                     ~ 0.242236024845,
              distrito == "CHORE"                       ~ 0.353398058252,
              distrito == "CIUDAD DEL ESTE"             ~ 0.311727383754,
              distrito == "CONCEPCION"                  ~ 0.370099196977,
              distrito == "CORONEL BOGADO"              ~ 0.368516833485,
              distrito == "CORONEL MARTINEZ"            ~ 0.291759465479,
              distrito == "CORONEL OVIEDO"              ~ 0.385099891743,
              distrito == "CORPUS CHRISTI"              ~ 0.315181518152,
              distrito == "DESMOCHADOS"                 ~ 0.401315789474,
              distrito == "DOMINGO MARTINEZ DE IRALA"   ~ 0.242647058824,
              distrito == "DR. BOTRELL"                 ~ 0.515625000000,
              distrito == "DR. CECILIO BAEZ"            ~ 0.379403794038,
              distrito == "DR. J. L. MALLORQUIN"        ~ 0.453908984831,
              distrito == "DR. JUAN M. FRUTOS"          ~ 0.363510711818,
              distrito == "DR. MOISES BERTONI"          ~ 0.473076923077,
              distrito == "DR. RAUL PEÑA"               ~ 0.277777777778,
              distrito == "DR.J. E. ESTIGARRIBIA"       ~ 0.344974187671,
              distrito == "EDELIRA"                     ~ 0.337365591398,
              distrito == "EEUU"                        ~ 0.123809523810,
              distrito == "EMBOSCADA"                   ~ 0.415847665848,
              distrito == "ENCARNACION"                 ~ 0.295748716804,
              distrito == "ESCOBAR"                     ~ 0.490333919156,
              distrito == "ESPAÑA"                      ~ 0.308571428571,
              distrito == "EUSEBIO AYALA"               ~ 0.439935064935,
              distrito == "FELIX PEREZ CARDOZO"         ~ 0.252173913043,
              distrito == "FERNANDO DE LA MORA"         ~ 0.317720646178,
              distrito == "FILADELFIA"                  ~ 0.286096256684,
              distrito == "FORTIN JOSE FALCON"          ~ 0.300940438871,
              distrito == "FRAM"                        ~ 0.305621536025,
              distrito == "FRANCISCO CABALLERO ALVAREZ" ~ 0.356725146199,
              distrito == "FRANCISCO SOLANO LOPEZ"      ~ 0.298642533937,
              distrito == "FUERTE OLIMPO"               ~ 0.376996805112,
              distrito == "FULGENCIO YEGROS"            ~ 0.309523809524,
              distrito == "GENERAL ARTIGAS"             ~ 0.308571428571,
              distrito == "GENERAL DELGADO"             ~ 0.282926829268,
              distrito == "GRAL. BERNARDINO CABALLERO"  ~ 0.329966329966,
              distrito == "GRAL. E. A. GARAY"           ~ 0.346504559271,
              distrito == "GRAL. ELIZARDO AQUINO"       ~ 0.397969543147,
              distrito == "GRAL. F. RESQUIN"            ~ 0.396067415730,
              distrito == "GRAL. JOSE E. DIAZ"          ~ 0.242105263158,
              distrito == "GRAL. JOSE M. BRUGUEZ"       ~ 0.500000000000,
              distrito == "GRAL. MORINIGO"              ~ 0.234299516908,
              distrito == "GUAJAYVI"                    ~ 0.280668257757,
              distrito == "GUARAMBARE"                  ~ 0.390849673203,
              distrito == "GUAZU CUA"                   ~ 0.324675324675,
              distrito == "HERNANDARIAS"                ~ 0.345928162475,
              distrito == "HOHENAU"                     ~ 0.354081033470,
              distrito == "HORQUETA"                    ~ 0.354414613894,
              distrito == "HUMAITA"                     ~ 0.145299145299,
              distrito == "INDEPENDENCIA"               ~ 0.305912596401,
              distrito == "IRUÑA"                       ~ 0.285714285714,
              distrito == "ISLA PUCU"                   ~ 0.337164750958,
              distrito == "ISLA UMBU"                   ~ 0.280172413793,
              distrito == "ITA"                         ~ 0.400000000000,
              distrito == "ITACURUBI DE LA CORDILLERA"  ~ 0.464607464607,
              distrito == "ITACURUBI DEL ROSARIO"       ~ 0.358288770053,
              distrito == "ITAKYRY"                     ~ 0.396160558464,
              distrito == "ITANARA"                     ~ 0.170212765957,
              distrito == "ITAPE"                       ~ 0.398753894081,
              distrito == "ITAPUA POTY"                 ~ 0.399250234302,
              distrito == "ITAUGUA"                     ~ 0.388071785184,
              distrito == "ITURBE"                      ~ 0.385658914729,
              distrito == "J. AUGUSTO SALDIVAR"         ~ 0.364134495641,
              distrito == "JESUS"                       ~ 0.426229508197,
              distrito == "JOSE A. FASSARDI"            ~ 0.329861111111,
              distrito == "JOSE DOMINGO OCAMPOS"        ~ 0.371308016878,
              distrito == "JOSE LEANDRO OVIEDO"         ~ 0.391459074733,
              distrito == "JUAN DE MENA"                ~ 0.398826979472,
              distrito == "JUAN E. O'LEARY"             ~ 0.306288032454,
              distrito == "KATUETE"                     ~ 0.334033613445,
              distrito == "LA COLMENA"                  ~ 0.320224719101,
              distrito == "LA PALOMA"                   ~ 0.379411764706,
              distrito == "LA PASTORA"                  ~ 0.337620578778,
              distrito == "LA PAZ"                      ~ 0.296296296296,
              distrito == "LAMBARE"                     ~ 0.338027771506,
              distrito == "LAURELES"                    ~ 0.287804878049,
              distrito == "LIBERACION"                  ~ 0.264059989288,
              distrito == "LIMA"                        ~ 0.404040404040,
              distrito == "LIMPIO"                      ~ 0.402033013149,
              distrito == "LOMA GRANDE"                 ~ 0.361058601134,
              distrito == "LOMA PLATA"                  ~ 0.210051546392,
              distrito == "LORETO"                      ~ 0.349286314022,
              distrito == "LOS CEDRALES"                ~ 0.321348314607,
              distrito == "LUQUE"                       ~ 0.378878782014,
              distrito == "MACIEL"                      ~ 0.416058394161,
              distrito == "MARIANO R. ALONSO"           ~ 0.369348762110,
              distrito == "MAYOR J. MARTINEZ"           ~ 0.373913043478,
              distrito == "MAYOR OTAÑO"                 ~ 0.253315649867,
              distrito == "MBARACAYU"                   ~ 0.210970464135,
              distrito == "MBOCAYATY DEL GUAIRA"        ~ 0.337016574586,
              distrito == "MBOCAYATY DEL YHAGUY"        ~ 0.383870967742,
              distrito == "MBUYAPEY"                    ~ 0.421436004162,
              distrito == "MCAL.ESTIGARRIBIA"           ~ 0.399705014749,
              distrito == "MINGA GUAZU"                 ~ 0.351938299168,
              distrito == "MINGA PORA"                  ~ 0.440891472868,
              distrito == "NANAWA"                      ~ 0.317518248175,
              distrito == "NARANJAL"                    ~ 0.314814814815,
              distrito == "NATALICIO TALAVERA"          ~ 0.384615384615,
              distrito == "NATALIO"                     ~ 0.362519201229,
              distrito == "NUEVA ALBORADA"              ~ 0.370179948586,
              distrito == "NUEVA COLOMBIA"              ~ 0.475000000000,
              distrito == "NUEVA ESPERANZA"             ~ 0.274876847291,
              distrito == "NUEVA GERMANIA"              ~ 0.359635811836,
              distrito == "NUEVA ITALIA"                ~ 0.341674687199,
              distrito == "NUEVA LONDRES"               ~ 0.389240506329,
              distrito == "NUEVA TOLEDO"                ~ 0.349206349206,
              distrito == "OBLIGADO"                    ~ 0.313960833860,
              distrito == "PARAGUARI"                   ~ 0.397229916898,
              distrito == "PASO DE PATRIA"              ~ 0.352941176471,
              distrito == "PASO YOBAI"                  ~ 0.312876052948,
              distrito == "PEDRO J. CABALLERO"          ~ 0.349989207857,
              distrito == "PILAR"                       ~ 0.317709652901,
              distrito == "PIRAPO"                      ~ 0.224795640327,
              distrito == "PIRAYU"                      ~ 0.388652482270,
              distrito == "PIRIBEBUY"                   ~ 0.390720390720,
              distrito == "PRESIDENTE FRANCO"           ~ 0.320781277494,
              distrito == "PUERTO CASADO"               ~ 0.334016393443,
              distrito == "PUERTO PINASCO"              ~ 0.395348837209,
              distrito == "QUIINDY"                     ~ 0.423542989036,
              distrito == "QUYQUYHO"                    ~ 0.409736308316,
              distrito == "R.I. 3 CORRALES"             ~ 0.268585131894,
              distrito == "RAUL ARSENIO OVIEDO"         ~ 0.353035143770,
              distrito == "REPATRIACION"                ~ 0.430213464696,
              distrito == "SALTO DEL GUAIRA"            ~ 0.363136176066,
              distrito == "SAN ALBERTO"                 ~ 0.323185011710,
              distrito == "SAN ANTONIO"                 ~ 0.328445330296,
              distrito == "SAN BERNARDINO"              ~ 0.358893777498,
              distrito == "SAN CARLOS DEL APA"          ~ 0.200000000000,
              distrito == "SAN COSME Y DAMIAN"          ~ 0.358669833729,
              distrito == "SAN CRISTOBAL"               ~ 0.269645608629,
              distrito == "SAN ESTANISLAO"              ~ 0.338594802695,
              distrito == "SAN IGNACIO"                 ~ 0.397492163009,
              distrito == "SAN ISIDRO CURUGUATY"        ~ 0.374598930481,
              distrito == "SAN JOAQUIN"                 ~ 0.412225705329,
              distrito == "SAN JOSE DE LOS ARROYOS"     ~ 0.412801484230,
              distrito == "SAN JOSE OBRERO"             ~ 0.262857142857,
              distrito == "SAN JUAN BAUTISTA"           ~ 0.473958333333,
              distrito == "SAN JUAN DEL PARANA"         ~ 0.273464658169,
              distrito == "SAN JUAN NEPOMUCENO"         ~ 0.260437375746,
              distrito == "SAN LAZARO"                  ~ 0.338777660695,
              distrito == "SAN LORENZO"                 ~ 0.360420601297,
              distrito == "SAN MIGUEL"                  ~ 0.410094637224,
              distrito == "SAN PABLO"                   ~ 0.492753623188,
              distrito == "SAN PATRICIO"                ~ 0.412878787879,
              distrito == "SAN PEDRO DEL PARANA"        ~ 0.301872659176,
              distrito == "SAN PEDRO DEL YCUAMANDYYU"   ~ 0.439750553209,
              distrito == "SAN RAFAEL DEL PARANA"       ~ 0.314049586777,
              distrito == "SAN ROQUE GONZALEZ"          ~ 0.291547277937,
              distrito == "SAN SALVADOR"                ~ 0.377777777778,
              distrito == "SANTA ELENA"                 ~ 0.330729166667,
              distrito == "SANTA FE DEL PARANA"         ~ 0.153526970954,
              distrito == "SANTA MARIA"                 ~ 0.409090909091,
              distrito == "SANTA RITA"                  ~ 0.291069924179,
              distrito == "SANTA ROSA"                  ~ 0.430806257521,
              distrito == "SANTA ROSA DEL AGUARAY"      ~ 0.376979528776,
              distrito == "SANTA ROSA DEL MBUTUY"       ~ 0.316666666667,
              distrito == "SANTA ROSA DEL MONDAY"       ~ 0.257270693512,
              distrito == "SANTIAGO"                    ~ 0.441540577717,
              distrito == "SAPUCAI"                     ~ 0.336254107338,
              distrito == "SGTO.JOSE FELIX LOPEZ"       ~ 0.248633879781,
              distrito == "SIMON BOLIVAR"               ~ 0.316702819957,
              distrito == "TACUARAS"                    ~ 0.275862068966,
              distrito == "TACUATI"                     ~ 0.391975308642,
              distrito == "TAVAI"                       ~ 0.342618384401,
              distrito == "TAVAPY"                      ~ 0.299047619048,
              distrito == "TEBICUARY"                   ~ 0.318568994889,
              distrito == "TEBICUARYMI"                 ~ 0.305647840532,
              distrito == "TEMBIAPORA"                  ~ 0.354771784232,
              distrito == "TOBATI"                      ~ 0.398763523957,
              distrito == "TOMAS ROMERO PEREIRA"        ~ 0.354223433243,
              distrito == "TRINIDAD"                    ~ 0.425959780622,
              distrito == "TTE. ESTEBAN MARTINEZ"       ~ 0.355140186916,
              distrito == "TTE. IRALA FERNANDEZ"        ~ 0.234042553191,
              distrito == "UNION"                       ~ 0.336448598131,
              distrito == "VALENZUELA"                  ~ 0.438923395445,
              distrito == "VAQUERIA"                    ~ 0.344019728730,
              distrito == "VILLA DEL ROSARIO"           ~ 0.400990099010,
              distrito == "VILLA ELISA"                 ~ 0.349962362280,
              distrito == "VILLA FLORIDA"               ~ 0.466858789625,
              distrito == "VILLA FRANCA"                ~ 0.351351351351,
              distrito == "VILLA HAYES"                 ~ 0.424021648626,
              distrito == "VILLA OLIVA"                 ~ 0.437837837838,
              distrito == "VILLA YGATIMI"               ~ 0.370771312585,
              distrito == "VILLALBIN"                   ~ 0.286666666667,
              distrito == "VILLARRICA"                  ~ 0.286840411840,
              distrito == "VILLETA"                     ~ 0.436895674300,
              distrito == "YAGUARON"                    ~ 0.378566203365,
              distrito == "YASY CAÑY"                   ~ 0.379022646007,
              distrito == "YATAITY DEL GUAIRA"          ~ 0.460815047022,
              distrito == "YATAITY DEL NORTE"           ~ 0.357771260997,
              distrito == "YATYTAY"                     ~ 0.404940923738,
              distrito == "YAVEVYRY"                    ~ 0.502793296089,
              distrito == "YBYCUI"                      ~ 0.342747726185,
              distrito == "YBYRAROVANA"                 ~ 0.303867403315,
              distrito == "YBYTYMI"                     ~ 0.426136363636,
              distrito == "YBYYAU"                      ~ 0.399168399168,
              distrito == "YGUAZU"                      ~ 0.236391912908,
              distrito == "YHU"                         ~ 0.349505840072,
              distrito == "YPACARAI"                    ~ 0.400576368876,
              distrito == "YPANE"                       ~ 0.388995675156,
              distrito == "YPE JHU"                     ~ 0.303643724696,
              distrito == "YRYBUCUA"                    ~ 0.378114842904,
              distrito == "YUTY"                        ~ 0.349522292994,
              distrito == "ZANJA PYTA"                  ~ 0.370517928287,
              distrito == "ÑACUNDAY"                    ~ 0.396296296296,
              distrito == "ÑEMBY"                       ~ 0.324318429662,
              distrito == "ÑUMI"                        ~ 0.131707317073,
              TRUE ~ 0
            ),
          scorecard == 0 ~
            case_when(
              distrito == 'DOMINGO MARTINEZ DE IRALA'   ~  70.51,
              distrito == '1RO. DE MARZO'               ~  16.67,
              distrito == '25 DE DICIEMBRE'             ~  -0.17,
              distrito == '3 DE FEBRERO'                ~  16.62,
              distrito == '3 DE MAYO'                   ~  50.44,
              distrito == 'ABAI'                        ~  82.46,
              distrito == 'ACAHAY'                      ~   7.69,
              distrito == 'ALBERDI'                     ~  41.65,
              distrito == 'ALTO VERA'                   ~  46.79,
              distrito == 'ALTOS'                       ~  -3.58,
              distrito == 'ANTEQUERA'                   ~  -4.23,
              distrito == 'AREGUA'                      ~ -21.01,
              distrito == 'ARGENTINA'                   ~   6.46,
              distrito == 'ARROYOS Y ESTEROS'           ~ -49.27,
              distrito == 'ASUNCION'                    ~  -8.76,
              distrito == 'ATYRA'                       ~ -52.73,
              distrito == 'AYOLAS'                      ~ -46.97,
              distrito == 'AZOTEY'                      ~  44.14,
              distrito == 'BELEN'                       ~ -10.65,
              distrito == 'BELLA VISTA'                 ~  51.56,
              distrito == 'BENJAMIN ACEVAL'             ~ -39.73,
              distrito == 'BORJA'                       ~  48.94,
              distrito == 'BUENA VISTA'                 ~  56.63,
              distrito == 'CAACUPE'                     ~ -48.78,
              distrito == 'CAAGUAZU'                    ~  13.22,
              distrito == 'CAAPUCU'                     ~  -5.32,
              distrito == 'CAAZAPA'                     ~ -11.96,
              distrito == 'CAMBYRETA'                   ~  36.45,
              distrito == 'CAP. MAURICIO JOSE TROCHE'   ~  84.27,
              distrito == 'CAPIATA'                     ~ -20.44,
              distrito == 'CAPIIVARY'                   ~  47.16,
              distrito == 'CAPITAN BADO'                ~  39.73,
              distrito == 'CAPITAN MEZA'                ~  76.17,
              distrito == 'CAPITAN MIRANDA'             ~  53.08,
              distrito == 'CARAGUATAY'                  ~  -2.47,
              distrito == 'CARAPEGUA'                   ~  50.55,
              distrito == 'CARAYAO'                     ~ -13.29,
              distrito == 'CARLOS ANTONIO LOPEZ'        ~  69.02,
              distrito == 'CARMELO PERALTA'             ~  38.01,
              distrito == 'CARMEN DEL PARANA'           ~  54.26,
              distrito == 'CERRITO'                     ~  91.42,
              distrito == 'CHORE'                       ~  19.31,
              distrito == 'CIUDAD DEL ESTE'             ~  18.36,
              distrito == 'CONCEPCION'                  ~  13.80,
              distrito == 'CORONEL BOGADO'              ~  35.38,
              distrito == 'CORONEL MARTINEZ'            ~  28.17,
              distrito == 'CORONEL OVIEDO'              ~ -10.64,
              distrito == 'CORPUS CHRISTI'              ~  53.79,
              distrito == 'DR. CECILIO BAEZ'            ~  -5.52,
              distrito == 'DR. J. L. MALLORQUIN'        ~   8.82,
              distrito == 'DR. JUAN M. FRUTOS'          ~  31.36,
              distrito == 'DR. MOISES BERTONI'          ~ -62.15,
              distrito == 'DR. RAUL PEÑA'               ~  67.15,
              distrito == 'DR.J. E. ESTIGARRIBIA'       ~  26.93,
              distrito == 'EDELIRA'                     ~  66.78,
              distrito == 'EEUU'                        ~ 149.81,
              distrito == 'EMBOSCADA'                   ~ -17.75,
              distrito == 'ENCARNACION'                 ~  51.21,
              distrito == 'ESCOBAR'                     ~ -10.28,
              distrito == 'ESPAÑA'                      ~  44.11,
              distrito == 'EUSEBIO AYALA'               ~ -35.14,
              distrito == 'FELIX PEREZ CARDOZO'         ~   6.82,
              distrito == 'FERNANDO DE LA MORA'         ~  -1.02,
              distrito == 'FILADELFIA'                  ~  54.95,
              distrito == 'FORTIN JOSE FALCON'          ~   9.63,
              distrito == 'FRAM'                        ~  66.40,
              distrito == 'FRANCISCO CABALLERO ALVAREZ' ~  -1.14,
              distrito == 'FRANCISCO SOLANO LOPEZ'      ~  30.37,
              distrito == 'FUERTE OLIMPO'               ~  78.95,
              distrito == 'FULGENCIO YEGROS'            ~  83.62,
              distrito == 'GENERAL ARTIGAS'             ~  61.67,
              distrito == 'GENERAL DELGADO'             ~  67.37,
              distrito == 'GRAL. BERNARDINO CABALLERO'  ~ -25.95,
              distrito == 'GRAL. E. A. GARAY'           ~  87.91,
              distrito == 'GRAL. ELIZARDO AQUINO'       ~  38.16,
              distrito == 'GRAL. F. RESQUIN'            ~   2.55,
              distrito == 'GRAL. JOSE M. BRUGUEZ'       ~ -22.71,
              distrito == 'GRAL. MORINIGO'              ~  60.17,
              distrito == 'GUAJAYVI'                    ~  50.81,
              distrito == 'GUARAMBARE'                  ~ -23.50,
              distrito == 'HERNANDARIAS'                ~  -3.54,
              distrito == 'HOHENAU'                     ~  63.41,
              distrito == 'HORQUETA'                    ~  37.46,
              distrito == 'HUMAITA'                     ~  64.80,
              distrito == 'INDEPENDENCIA'               ~  43.88,
              distrito == 'IRUÑA'                       ~  55.43,
              distrito == 'ISLA PUCU'                   ~  17.84,
              distrito == 'ISLA UMBU'                   ~  89.93,
              distrito == 'ITA'                         ~ -17.94,
              distrito == 'ITACURUBI DE LA CORDILLERA'  ~ -15.70,
              distrito == 'ITACURUBI DEL ROSARIO'       ~ -24.74,
              distrito == 'ITAKYRY'                     ~  28.81,
              distrito == 'ITAPE'                       ~   1.07,
              distrito == 'ITAPUA POTY'                 ~  41.25,
              distrito == 'ITAUGUA'                     ~ -18.79,
              distrito == 'ITURBE'                      ~  34.77,
              distrito == 'J. AUGUSTO SALDIVAR'         ~ -21.52,
              distrito == 'JESUS'                       ~  26.82,
              distrito == 'JOSE A. FASSARDI'            ~ -33.59,
              distrito == 'JOSE DOMINGO OCAMPOS'        ~  44.95,
              distrito == 'JOSE LEANDRO OVIEDO'         ~  43.73,
              distrito == 'JUAN DE MENA'                ~  28.31,
              distrito == 'KATUETE'                     ~  36.70,
              distrito == 'LA COLMENA'                  ~  29.84,
              distrito == 'LA PALOMA'                   ~  37.17,
              distrito == 'LA PASTORA'                  ~   0.10,
              distrito == 'LA PAZ'                      ~  92.56,
              distrito == 'LAMBARE'                     ~   9.05,
              distrito == 'LAURELES'                    ~  36.03,
              distrito == 'LIBERACION'                  ~  67.90,
              distrito == 'LIMA'                        ~  17.40,
              distrito == 'LIMPIO'                      ~  -8.05,
              distrito == 'LOMA GRANDE'                 ~  -5.18,
              distrito == 'LOMA PLATA'                  ~  85.76,
              distrito == 'LORETO'                      ~  19.51,
              distrito == 'LOS CEDRALES'                ~  20.62,
              distrito == 'LUQUE'                       ~ -18.94,
              distrito == 'MACIEL'                      ~ -16.01,
              distrito == 'MARIANO R. ALONSO'           ~  -9.04,
              distrito == 'MAYOR J. MARTINEZ'           ~  52.58,
              distrito == 'MAYOR OTAÑO'                 ~  82.19,
              distrito == 'MBARACAYU'                   ~ 115.24,
              distrito == 'MBOCAYATY DEL GUAIRA'        ~  -7.58,
              distrito == 'MBOCAYATY DEL YHAGUY'        ~  40.34,
              distrito == 'MBUYAPEY'                    ~   3.18,
              distrito == 'MCAL.ESTIGARRIBIA'           ~  10.65,
              distrito == 'MINGA GUAZU'                 ~  16.32,
              distrito == 'MINGA PORA'                  ~  38.37,
              distrito == 'ÑACUNDAY'                    ~  34.42,
              distrito == 'NANAWA'                      ~  59.57,
              distrito == 'NARANJAL'                    ~  84.56,
              distrito == 'NATALICIO TALAVERA'          ~  61.96,
              distrito == 'NATALIO'                     ~  53.98,
              distrito == 'ÑEMBY'                       ~  15.94,
              distrito == 'NUEVA ALBORADA'              ~  24.54,
              distrito == 'NUEVA COLOMBIA'              ~ -29.82,
              distrito == 'NUEVA ESPERANZA'             ~  32.45,
              distrito == 'NUEVA GERMANIA'              ~ -19.50,
              distrito == 'NUEVA ITALIA'                ~  28.77,
              distrito == 'NUEVA LONDRES'               ~  43.73,
              distrito == 'NUEVA TOLEDO'                ~ 133.11,
              distrito == 'ÑUMI'                        ~  74.15,
              distrito == 'OBLIGADO'                    ~  40.96,
              distrito == 'otros'                       ~  46.63,
              distrito == 'PARAGUARI'                   ~  -7.38,
              distrito == 'PASO YOBAI'                  ~  28.37,
              distrito == 'PEDRO J. CABALLERO'          ~  24.70,
              distrito == 'PILAR'                       ~  19.14,
              distrito == 'PIRAPO'                      ~  43.03,
              distrito == 'PIRAYU'                      ~ -49.33,
              distrito == 'PIRIBEBUY'                   ~ -32.55,
              distrito == 'PRESIDENTE FRANCO'           ~   3.82,
              distrito == 'PUERTO CASADO'               ~  33.41,
              distrito == 'PUERTO PINASCO'              ~ -15.05,
              distrito == 'QUIINDY'                     ~   1.77,
              distrito == 'QUYQUYHO'                    ~  -1.80,
              distrito == 'R.I. 3 CORRALES'             ~  41.19,
              distrito == 'RAUL ARSENIO OVIEDO'         ~ 102.51,
              distrito == 'REPATRIACION'                ~  22.55,
              distrito == 'SALTO DEL GUAIRA'            ~   9.88,
              distrito == 'SAN ALBERTO'                 ~  59.42,
              distrito == 'SAN ANTONIO'                 ~   7.84,
              distrito == 'SAN BERNARDINO'              ~ -12.90,
              distrito == 'SAN COSME Y DAMIAN'          ~  32.70,
              distrito == 'SAN CRISTOBAL'               ~ 101.26,
              distrito == 'SAN ESTANISLAO'              ~  38.60,
              distrito == 'SAN IGNACIO'                 ~ -13.12,
              distrito == 'SAN ISIDRO CURUGUATY'        ~  19.05,
              distrito == 'SAN JOAQUIN'                 ~  -0.21,
              distrito == 'SAN JOSE DE LOS ARROYOS'     ~ -17.15,
              distrito == 'SAN JOSE OBRERO'             ~  17.29,
              distrito == 'SAN JUAN BAUTISTA'           ~ -56.72,
              distrito == 'SAN JUAN DEL PARANA'         ~  31.08,
              distrito == 'SAN JUAN NEPOMUCENO'         ~  47.06,
              distrito == 'SAN LAZARO'                  ~  34.72,
              distrito == 'SAN LORENZO'                 ~ -10.54,
              distrito == 'SAN MIGUEL'                  ~ -60.35,
              distrito == 'SAN PABLO'                   ~ -63.15,
              distrito == 'SAN PATRICIO'                ~ -34.70,
              distrito == 'SAN PEDRO DEL PARANA'        ~  66.66,
              distrito == 'SAN PEDRO DEL YCUAMANDYYU'   ~  -5.25,
              distrito == 'SAN RAFAEL DEL PARANA'       ~  64.76,
              distrito == 'SAN ROQUE GONZALEZ'          ~  60.23,
              distrito == 'SAN SALVADOR'                ~   3.18,
              distrito == 'SANTA ELENA'                 ~ -12.36,
              distrito == 'SANTA FE DEL PARANA'         ~  92.89,
              distrito == 'SANTA MARIA'                 ~ -15.05,
              distrito == 'SANTA RITA'                  ~  55.55,
              distrito == 'SANTA ROSA'                  ~ -31.99,
              distrito == 'SANTA ROSA DEL AGUARAY'      ~   8.66,
              distrito == 'SANTA ROSA DEL MBUTUY'       ~  -0.59,
              distrito == 'SANTA ROSA DEL MONDAY'       ~ 223.82,
              distrito == 'SANTIAGO'                    ~  -5.34,
              distrito == 'SAPUCAI'                     ~  -4.99,
              distrito == 'SGTO.JOSE FELIX LOPEZ'       ~  33.26,
              distrito == 'SIMON BOLIVAR'               ~   6.63,
              distrito == 'TACUARAS'                    ~  11.18,
              distrito == 'TACUATI'                     ~ -32.03,
              distrito == 'TAVAI'                       ~  79.16,
              distrito == 'TAVAPY'                      ~  28.13,
              distrito == 'TEBICUARY'                   ~  74.85,
              distrito == 'TEBICUARYMI'                 ~  41.05,
              distrito == 'TEMBIAPORA'                  ~   9.37,
              distrito == 'TOBATI'                      ~ -17.95,
              distrito == 'TOMAS ROMERO PEREIRA'        ~  52.52,
              distrito == 'TRINIDAD'                    ~  13.23,
              distrito == 'TTE. IRALA FERNANDEZ'        ~  83.84,
              distrito == 'UNION'                       ~  -4.10,
              distrito == 'VALENZUELA'                  ~ -52.18,
              distrito == 'VAQUERIA'                    ~  24.28,
              distrito == 'VILLA DEL ROSARIO'           ~ -19.42,
              distrito == 'VILLA ELISA'                 ~  -6.61,
              distrito == 'VILLA FLORIDA'               ~ -27.10,
              distrito == 'VILLA HAYES'                 ~ -23.70,
              distrito == 'VILLA OLIVA'                 ~  13.72,
              distrito == 'VILLA YGATIMI'               ~  19.69,
              distrito == 'VILLARRICA'                  ~  16.38,
              distrito == 'VILLETA'                     ~  -9.14,
              distrito == 'YAGUARON'                    ~ -29.63,
              distrito == 'YASY CAÑY'                   ~   5.17,
              distrito == 'YATAITY DEL GUAIRA'          ~  -5.52,
              distrito == 'YATAITY DEL NORTE'           ~  14.06,
              distrito == 'YATYTAY'                     ~  41.57,
              distrito == 'YAVEVYRY'                    ~ -10.17,
              distrito == 'YBYCUI'                      ~  30.23,
              distrito == 'YBYRAROVANA'                 ~  24.54,
              distrito == 'YBYTYMI'                     ~  -4.74,
              distrito == 'YBYYAU'                      ~   5.92,
              distrito == 'YGUAZU'                      ~  38.13,
              distrito == 'YHU'                         ~  56.12,
              distrito == 'YPACARAI'                    ~ -24.45,
              distrito == 'YPANE'                       ~  -3.98,
              distrito == 'YPE JHU'                     ~  51.36,
              distrito == 'YRYBUCUA'                    ~  30.62,
              distrito == 'YUTY'                        ~  65.08,
              distrito == 'ZANJA PYTA'                  ~ 108.79,
              TRUE ~ 42.09
            ),
        ),
      
    )
  
  
  # Score Num
  df <- df %>%
    mutate(
      logodds =
        case_when(
          scorecard == 1 ~
            0.11877     +
            0.208842    * flag_mnx                      +
            0.07803     * g_max_venc_ten_36             +
            0.013473    * inf_cant_dist_afi_u6m         +
            -0.022072    * ich_cons_cant_banco_admin     +
            0.243027    * inf_morce_cant_61_90_abiertas +
            0.161477    * inf_morce_cant_31_45_abiertas +
            0.19876     * inf_morce_cant_46_60_abiertas +
            0.002054    * tc_max_dias_atraso_u6m        +
            -0.001317    * tc_primera_act                +
            0.041465    * p_sdot_monto_gt75_m1          +
            -0.000058756 * inf_cons_ult_dias_ccredito    +
            0.352326    * tc_prom_pmin_limcr_m1         +
            0.012349    * p_cant_atraso                 +
            -0.001048    * situacion_bancaria_woe        +
            0.033786    * inf_cant_dist_afi_ccred_u3m   +
            -0.000165    * inf_cons_ult_dias_sf          +
            -0.00154     * edad                          +
            0.054837    * tc_svenc_sdot_gt0_u12m        +
            0.416607    * flag_judiciales               +
            0.393459    * distrito_woe                  +
            0.097004    * ich_ch_re_cant_i_fond         +
            0.111114    * flag_saldo_vencido_ult_gt0    +
            0.004891    * inf_cons_cant_total_telco     +
            -0.008784    * tc_close_total                +
            0.011588    * ips_flag_actividad,
          scorecard == 0 ~
            0.216635           +
            0.030149           * inf_cant_dist_afi_u6m         +
            0.487934           * inf_morce_cant_abiertas       +
            -0.002178           * edad                          +
            -0.005569           * ich_cons_cant_total_recod     +
            0.067513           * flag_mnx                      +
            0.028268           * inf_cons_cant_u1m             +
            0.626119           * flag_judiciales               +
            -0.000669           * distrito_woe                  +
            0.0000000004646968 * fecha_ult_cons_privdia        +
            0.024178           * inf_cant_dist_afi_telco_u12m  +
            -0.000082265        * inf_cons_ult_dias_sf          +
            -0.000050311        * inf_cons_ult_dias_ccredito    +
            -0.027242           * casado                        +
            -0.023956           * ich_cons_cant_banco_admin     +
            0.065592           * inf_morce_cant_61_90_abiertas +
            0.024107           * inf_cons_cant_telco_u1m
        ),
      
      score_pos = round((1 / (1 + exp(logodds))) * 1000),
      score_pos = round(2.018442623 * score_pos + -179.6413934),
      # flow X
      score_pos =
        case_when(score_pos > 999 ~ 999, score_pos < 1 ~ 1, TRUE ~ score_pos),
      score_pos = case_when(flow_x == 1 ~ -1, TRUE ~ score_pos)
      
    )
  
  # Letra
  df <- df %>%
    mutate(
      flow_pos =
        case_when(
          score_pos > 805 & score_pos <= 999 ~ 'A',
          score_pos > 781 & score_pos <= 805 ~ 'B',
          score_pos > 763 & score_pos <= 781 ~ 'C',
          score_pos > 749 & score_pos <= 763 ~ 'D',
          score_pos > 733 & score_pos <= 749 ~ 'E',
          score_pos > 719 & score_pos <= 733 ~ 'F',
          score_pos > 708 & score_pos <= 719 ~ 'G',
          score_pos > 696 & score_pos <= 708 ~ 'H',
          score_pos > 684 & score_pos <= 696 ~ 'I',
          score_pos > 664 & score_pos <= 684 ~ 'J',
          score_pos > 606 & score_pos <= 664 ~ 'K',
          score_pos > 527 & score_pos <= 606 ~ 'L',
          score_pos > 432 & score_pos <= 527 ~ 'M',
          score_pos >   0 & score_pos <= 432 ~ 'N',
          score_pos == -1 ~ 'X'
        )
    )
  
  
  if (!is.null(idx_columns))
    return(df %>% select(c(idx_columns, 'score_pos', 'flow_pos')))
  
  return(df)
}

cluster_name_connect <- function(type = "Impala") {
  #https://plumbr.io/outofmemoryerror/gc-overhead-limit-exceeded
  #ver java -Xmx10m -XX:+UseParallelGC Wrapper
  #options(java.parameters = "-Xmx140g", java.parameters = "-XX:+UseParallelGC")
  
  .jinit(
    classpath = c(
      list.files(
        path = '/opt/cloudera/parcels/CDH/lib/hive/lib',
        patter = 'jar',
        full.names = TRUE),
      list.files(
        path = '/opt/cloudera/parcels/CDH/lib/hadoop/lib',
        pattern = 'jar',
        full.names = TRUE),
      list.files(
        path = '/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/lib',
        pattern = 'jar',
        full.names = TRUE),
      list.files(
        path = '/opt/cloudera/parcels/CDH/lib/hadoop',
        pattern = 'jar',
        full.names = TRUE)
    ),
    parameters = '-Djavax.security.auth.useSubjectCredsOnly=false'
  )
  
  drv <-
    JDBC(
      driverClass = 'org.apache.hive.jdbc.HiveDriver',
      classPath = 'hive-jdbc.jar',
      identifier.quote = '`'
    )
  
  if (trimws(tolower(type)) == tolower('Impala')) {
    dbConnect(
      drv,
      paste0('jdbc:hive2://node004.eis.company.com:21050/;',
             'principal=impala/node004@EDA.company.COM')
    )
  } else {
    dbConnect(
      drv,
      paste0('jdbc:hive2://node002.eis.company.com:10000/;',
             'principal=hive/_HOST@EDA.company.COM')
    )
  }
}

# Extraer datos necesarios de cbr ----
#conn <- cluster_name_connect("hive")
conn <- cluster_name_connect()
res <-
  c(
    idx_columns,
    variables.flow.x,
    variables.flow.pos
  ) %>%
  unique %>% paste(collapse = ',') %>%
  paste('select', ., 'from', paste0(db_indi, '.', tb_indi)) %>%
  #paste('select', ., 'from', paste0(db_indi, '.', tb_indi,' limit 40')) %>%
  dbGetQuery(conn, .)
dbDisconnect(conn)

# Cálculo de las flows
res <- res %>% left_join(calcular.flow.pos(., idx_columns))

# Guardar los resultados

#Reemplazar los NA por NULL para guardado en la base
#OBS: dejar asi y usar data.table que deja los NA en vacios.
#Luego decir a la base que los vacios los tome como NULLs
#res[is.na(res)] <- 'NULL'     

#archive <- format(as.Date(Sys.Date(), format = '%Y%m%d'), "%Y%m%d")
#path='/export/home/user/R'
#file_name='score_flow'

printf("Escribiendo archivo %s_%s.csv\n",file_name,archive)

#Con dataframe tarda casi 2 minutos (114.540 seg)
#system.time(write.csv(res,file=sprintf("%s/%s.csv",path,file_name)))

#Con data.table tarda 8 segs !!
library(data.table)
#system.time(fwrite(res,file=sprintf("%s/%s_%s.csv",path,file_name,archive)))
#fwrite(res,file=sprintf("%s/%s_%s.csv",path,file_name,archive))

# Using ';' or '|' as separator (very few pers_id has ',' in it!!)
#fwrite(res,file=sprintf("%s/%s_%s.csv",path,file_name,archive),sep = ";")
fwrite(res,file=sprintf("%s/%s_%s.csv",path,file_name,archive),sep = "|")

end_time <- Sys.time()
total_time <- round((end_time - start_time),2)

printf("#    Start Time:%s\tEnd Time: %s\tTotal Time to complete:%s\t#\n\n", start_time, end_time, total_time)

######## --FIN de Score flow (+) -- ########
