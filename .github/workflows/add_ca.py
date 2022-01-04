import certifi

cafile = certifi.where()
print(f"Certifi file path: '{cafile}'")
with open("./certs/ca.crt", "rb") as infile:
    customca = infile.read()
with open(cafile, "ab") as outfile:
    outfile.write(customca)
