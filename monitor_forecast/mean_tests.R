# mean test


# parameters of generalized logistic distribution

era_params$wb_rollsum3 |> 
  slice(L, 1) |> 
  as_tibble() |> 
  filter(!is.na(xi)) -> df


df_1row <- slice_sample(df, n = 1)
real <- lmom::quaglo(runif(500), c(df_1row$xi, df_1row$alpha, df_1row$k)) |> mean() |> round(3)
copilot <- round(df_1row$xi + df_1row$alpha / (1 - 1 / (1 + df_1row$k)),3)
chgpt <- round(df_1row$xi + df_1row$alpha * (gamma(1 - 1 / df_1row$k) / gamma(1 + 1 / df_1row$k)),3)
claud <- round(df_1row$xi + (df_1row$alpha / df_1row$k) * (digamma(1) - digamma(1/df_1row$k)) ,3)
location <- round(df_1row$xi, 3)

print(str_glue("real: {real}"))
print(str_glue("copilot: {copilot}"))
print(str_glue("chatgpt: {chgpt}"))
print(str_glue("claude: {claud}"))
print(str_glue("loc: {location}"))







