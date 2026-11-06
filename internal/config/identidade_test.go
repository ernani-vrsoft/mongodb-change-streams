package config

import "testing"

// A identidade tem que ignorar senha e parâmetros: girar a senha ou
// acrescentar uma opção na URI não pode custar uma carga completa.
func TestSourceIdentityIgnoraCredencialEParametros(t *testing.T) {
	mesmas := []string{
		"mongodb://nuvem.vrsoft.com.br:27017/?directConnection=true",
		"mongodb://usuario:senha@nuvem.vrsoft.com.br:27017/",
		"mongodb://usuario:OUTRASENHA@nuvem.vrsoft.com.br:27017/?maxPoolSize=5",
		"mongodb://nuvem.vrsoft.com.br:27017/pdv?replicaSet=rs0",
		"MongoDB://NUVEM.vrsoft.com.br:27017/",
	}

	esperada := SourceIdentity(mesmas[0])
	for _, uri := range mesmas[1:] {
		if got := SourceIdentity(uri); got != esperada {
			t.Errorf("SourceIdentity(%q) = %q, esperado %q", uri, got, esperada)
		}
	}
}

// Servidor diferente tem que dar identidade diferente: é isso que dispara a
// carga completa e evita misturar dois bancos no PDV.
func TestSourceIdentityDistingueServidores(t *testing.T) {
	diferentes := []string{
		"mongodb://nuvem-a.vrsoft.com.br:27017/",
		"mongodb://nuvem-b.vrsoft.com.br:27017/",
		"mongodb://nuvem-a.vrsoft.com.br:27018/",
		"mongodb+srv://cluster0.abcde.mongodb.net/",
		"mongodb://host.docker.internal:27010/",
		"mongodb://host.docker.internal:27011/",
	}

	vistas := map[string]string{}
	for _, uri := range diferentes {
		id := SourceIdentity(uri)
		if anterior, repetida := vistas[id]; repetida {
			t.Errorf("%q e %q deram a mesma identidade %q", anterior, uri, id)
		}
		vistas[id] = uri
	}
}

// A ordem dos hosts de um replica set não muda o servidor.
func TestSourceIdentityIgnoraOrdemDosHosts(t *testing.T) {
	a := SourceIdentity("mongodb://n1:27017,n2:27017,n3:27017/?replicaSet=rs0")
	b := SourceIdentity("mongodb://n3:27017,n1:27017,n2:27017/?replicaSet=rs0")
	if a != b {
		t.Errorf("ordem dos hosts mudou a identidade: %q != %q", a, b)
	}
}
