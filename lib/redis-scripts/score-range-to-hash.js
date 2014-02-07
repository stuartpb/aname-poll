module.exports = function() {
  return ["local c=redis.call",
    "local u=unpack or table.unpack",
    "local s=c('zrangebyscore',KEYS[1],ARGV[1],ARGV[2],'withscores')",
    "if #s>0 then",
      "c('hmset',KEYS[2],u(s))",
      "local k={}",
      "for i=2,#s,2 do",
        "k[i/2]=s[i-1]",
      "end",
      "c('zrem',KEYS[1],u(k))",
    "end",
    "return s"
  ].join('\n');
};
