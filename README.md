# Discords-best-giveaway-bot-v5
best giveaway bot, has normal giveaways, split or steal, double or keep, maze runner and more.



when running please replace the last chunk with

if __name__ == '__main__':
    TOKEN_OVERRIDE = "ur token"
    token = TOKEN_OVERRIDE or os.environ.get('discord_token')
    if not token:
        print('Set DISCORD_TOKEN environment variable')
    else:
        bot.run(token)

