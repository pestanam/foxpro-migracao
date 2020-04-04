package br.com.dbFox.migracao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jacob.com.Variant;

public class Main {

	private static String connectStr = "Provider=vfpoledb;Data Source=C:\\NET\\workspaces\\foxpro\\dados; Sequence=general;\";";

	public static void printFavorecido(Recordset rs) {
		Fields fs = rs.getFields();
		StringBuilder sbInsertFavorecido = new StringBuilder();

		sbInsertFavorecido.append(
				"INSERT INTO favorecido (codigo, descricao, complemento, valor_fixo) values (");

		rs.MoveFirst();
		while (!rs.getEOF()) {
			StringBuilder sbCommandFavorecido = new StringBuilder();
			sbCommandFavorecido.append(sbInsertFavorecido);

			for (int i = 0; i < fs.getCount(); i++) {
				Field f = fs.getItem(i);
				Variant v = f.getValue();

				if (i == 0) {
					int codigo = v.changeType(Variant.VariantInt).getInt();
					sbCommandFavorecido.append(codigo);
				}

				if (i == 1 || i == 2) {
					sbCommandFavorecido.append("'").append(v.toString().trim()).append("'");
				} 
				
				if (i == 3) {
					String valor = v.toString().trim();
					sbCommandFavorecido.append(valor.equals("") ? "null" : valor);
				}

				if (i < fs.getCount() - 1) {
					sbCommandFavorecido.append(",");
				}

				// System.out.print(v + ";");
			}

			sbCommandFavorecido.append(");");
			System.out.println(sbCommandFavorecido.toString());
			rs.MoveNext();
		}

	}
	
	public static void printObreiro(Recordset rs) {
		Fields fs = rs.getFields();
		StringBuilder sbInsertObreiro = new StringBuilder();
		StringBuilder sbInsertContato = new StringBuilder();

		sbInsertObreiro.append(
				"INSERT INTO obreiro (codigo, nome, status, data_inicio_licenca, data_fim_licenca, codigo_titulo, identificacao_bancaria, codigo_tipo_envio,cpf) values (");

		sbInsertContato.append("INSERT INTO contato (email, telefone_fixo, telefone_celular, codigo_obreiro) values (");

		rs.MoveFirst();
		while (!rs.getEOF()) {
			StringBuilder sbCommandObreiro = new StringBuilder();
			StringBuilder sbCommandContato = new StringBuilder();
			sbCommandObreiro.append(sbInsertObreiro);
			int status = 0;
			int codigo_obreiro = 0;
			String nome = null;

			List<String> contatos = new ArrayList<String>();

			for (int i = 0; i < fs.getCount(); i++) {
				Field f = fs.getItem(i);
				Variant v = f.getValue();

				if (i == 7 || i == 8 || i == 9 || i == 10 || i == 11) {
					continue;
				}

				if (i == 0) {
					codigo_obreiro = v.changeType(Variant.VariantInt).getInt();
				}

				if (i == 1) {
					sbCommandObreiro.append("'").append(v.toString().trim()).append("'");
					nome = v.toString().trim();
				} else if ((i == 3 || i == 4)) { // Datas Licenca

					if (!v.toString().equals("null")) {
						DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
						Date data = v.changeType(Variant.VariantDate).getJavaDate();

						sbCommandObreiro.append("'").append(formatter.format(data)).append("'");
					} else {
						sbCommandObreiro.append("null");
					}
				} else if (v.toString().equals("null")) {
					sbCommandObreiro.append("''");
				} else if (i == 2) {// status
					status = v.changeType(Variant.VariantInt).getInt();
					sbCommandObreiro.append(v);
				} else if (i == 5) {// Titulo

					if (status == 1 || status == 3) { // Obreiro Ativo / Inativo

						if (v == null || v.toString().equals("null") || v.toString().equals(" ")) { // Obreiro Normal
							sbCommandObreiro.append(1);

						} else if (v.toString().equals("E")) { // Emerito Ativo
							sbCommandObreiro.append(3);

						} else if (v.toString().equals("P")) { // Provecto Ativo
							sbCommandObreiro.append(4);

						}

					} else if (status == 2) { // Obreiro Licenciado
						if (v == null || v.toString().equals("null") || v.toString().equals(" ")) { // Obreiro
																									// Licenciado
							sbCommandObreiro.append(2);
						}
					} else if (status == 4) {
						sbCommandObreiro.append(5);
					}

				} else if (i == 13) { // email
					String[] email = v.toString().trim().split(";");
					for (int j = 0; j < email.length; j++) {
						if (!email[j].equals("")) {
							sbCommandContato = new StringBuilder();
							sbCommandContato.append(sbInsertContato);
							sbCommandContato.append("'").append(email[j]).append("'").append(",''").append(",'',")
									.append(codigo_obreiro).append(");");
							contatos.add(sbCommandContato.toString());
						}
					}
				} else if (i == 12) {// Tipo Envio
					int codigo = v.changeType(Variant.VariantInt).getInt();
					if (codigo > 0) {
						sbCommandObreiro.append(codigo);
					} else {
						sbCommandObreiro.append(3);
					}

				} else if (i == 6) {// Ientificacao Bancaria
					int codigo = v.changeType(Variant.VariantInt).getInt();
					if (codigo > 0) {
						sbCommandObreiro.append(codigo);
					} else {
						sbCommandObreiro.append("null");
					}
				} else {
					sbCommandObreiro.append(v);
				}

				if (i < fs.getCount() - 1) {
					sbCommandObreiro.append(",");
				}

				// System.out.print(v + ";");
			}

			populaCPF(nome, sbCommandObreiro);
			sbCommandObreiro.append(");");
			System.out.println(sbCommandObreiro.toString());
			for (String contato : contatos) {
				System.out.println(contato);
			}
			// System.out.println("");
			rs.MoveNext();
		}

	}

	public static void printConta(Recordset rs) {
		Fields fs = rs.getFields();
		StringBuilder sbInsert = new StringBuilder();
		StringBuilder soInsertRel = new StringBuilder();

		sbInsert.append(
				"INSERT INTO conta (codigo, descricao, tipo_transacao, tipo_relacionamento, efetua_lancamento_obreiro, efetua_lancamento_razao) values (");

		soInsertRel.append("INSERT INTO rel_conta_tipo_razao (codigo_tipo_razao, codigo_conta) values (");

		rs.MoveFirst();
		while (!rs.getEOF()) {
			StringBuilder sbCommand = new StringBuilder();
			StringBuilder sbCommandRel = new StringBuilder();

			sbCommand.append(sbInsert);
			String codigo = "";
			List<String> rels = new ArrayList<String>();

			for (int i = 0; i < fs.getCount(); i++) {
				Field f = fs.getItem(i);
				Variant v = f.getValue();

				if (i == 0) {
					codigo = v.toString().trim();
				}

				if (i == 0 || i == 1 || i == 2) {
					sbCommand.append("'").append(v.toString().trim()).append("'");
				}

				if (i == 3) {
					String tipo = v.toString().trim();

					if (tipo.equals("COBR")) {
						sbCommand.append("1").append(",'").append("S").append("',").append("'").append("N").append("'");
					} else if (tipo.equals("LFA1")) {
						sbCommand.append("2").append(",'").append("N").append("',").append("'").append("S").append("'");
					} else {
						sbCommand.append("0,").append("null,").append("null");
					}

				}

				if (i == 4) {
					String[] contas = v.changeType(Variant.VariantString).getString().split("(?<=\\G.{1})");
					for (int j = 0; j < contas.length; j++) {
						sbCommandRel = new StringBuilder();
						if (contas[j] != null && !contas[j].equals(" ") && !contas[j].equals("")) {
							sbCommandRel.append(soInsertRel);
							sbCommandRel.append("'").append(contas[j]).append("',").append("'").append(codigo)
									.append("');");
							rels.add(sbCommandRel.toString());
						}
					}
				}

				if (i < fs.getCount() - 2) {
					sbCommand.append(",");
				}

				// System.out.print(v + ";");
			}
			sbCommand.append(");");
			System.out.println(sbCommand.toString());
			for (String rel : rels) {
				System.out.println(rel);
			}
			// System.out.println("");
			rs.MoveNext();
		}

	}

	public static void printCCObreiro(Recordset rs) {
		Fields fs = rs.getFields();
		StringBuilder sbInsert = new StringBuilder();

		sbInsert.append(
				"INSERT INTO cj81.lancamento (categoria_lancamento, codigo_conta, data_lancamento, codigo_obreiro, descricao, valor, identificacao_bancaria) values ('O', ");

		rs.MoveFirst();
		while (!rs.getEOF()) {
			StringBuilder sbCommand = new StringBuilder();
			sbCommand.append(sbInsert);

			for (int i = 0; i < fs.getCount(); i++) {
				Field f = fs.getItem(i);

				Variant v = f.getValue();

				if (i == 0 || i == 3) {
					sbCommand.append("'").append(v.toString().trim()).append("'");
				} else if (i == 1) {
					DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
					sbCommand.append("'").append(formatter.format(v.changeType(Variant.VariantDate).getJavaDate()))
							.append("'");
				} else if (i == 5) {
					String valor = v.changeType(Variant.VariantString).getString().trim();
					sbCommand.append(valor.equals("") ? "null" : "'" + valor + "'");
				} else if (i == 2 || i == 4){
					String valor = v.toString().trim();
					sbCommand.append(valor.equals("") ? "null" : valor);
				}

				if (i < fs.getCount() - 2) {
					sbCommand.append(",");
				}

				// System.out.print(v + ";");
			}
			sbCommand.append(");");
			System.out.println(sbCommand.toString());
			rs.MoveNext();
		}

	}

	public static void printCCRazao(Recordset rs) {
		Fields fs = rs.getFields();
		StringBuilder sbInsert = new StringBuilder();

		sbInsert.append(
				"INSERT INTO cj81.lancamento (categoria_lancamento, codigo_tipo_razao, codigo_conta, data_lancamento, codigo_obreiro,codigo_favorecido, descricao, valor) values ('R', ");

		rs.MoveFirst();
		while (!rs.getEOF()) {
			StringBuilder sbCommand = new StringBuilder();
			sbCommand.append(sbInsert);
			String conta = "";
			for (int i = 0; i < fs.getCount(); i++) {
				Field f = fs.getItem(i);

				Variant v = f.getValue();

				if (i == 0) { // Tipo Razao
					sbCommand.append(v.changeType(Variant.VariantInt).getInt());
				} else if (i == 1) { // Conta
					conta = v.toString();
					sbCommand.append("'").append(conta).append("'");
				} else if (i == 2) { // Data Lancamento
					DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
					Date data = v.changeType(Variant.VariantDate).getJavaDate();

					sbCommand.append("'").append(formatter.format(data)).append("'");
				} else if (i == 3) { // Obreiro / Favorecido
					if (conta.equals("P0001") || conta.equals("T0001") || conta.equals("T0002")) {
						sbCommand.append("null").append(",");
						sbCommand.append(v.changeType(Variant.VariantInt).getInt());
					} else if (conta.equals("A0001") || conta.equals("A0002") || conta.equals("A0003")
							|| conta.equals("A0004") || conta.equals("C0009") || conta.equals("M0000")
							|| conta.equals("M0001") || conta.equals("M0002") || conta.equals("M0003")
							|| conta.equals("M0004") || conta.equals("M0005") || conta.equals("M0006")
							|| conta.equals("M0007") || conta.equals("M0008")) {
						sbCommand.append(v.changeType(Variant.VariantInt).getInt()).append(",");
						sbCommand.append("null");
					} else {
						sbCommand.append("null").append(",");
						sbCommand.append("null");
					}
				} else if (i==4) { // descricao
					sbCommand.append("'").append((v.toString().trim()).replace("'", "''")).append("'");
				} else if (i==5) { // valor / sequencia
					sbCommand.append(v);
//				} else if (i==5 || i==6) { // valor / sequencia
//					sbCommand.append(v);
				}

				if (i < fs.getCount() - 2) {
					sbCommand.append(",");
				}

				// System.out.print(v + ";");
			}
			sbCommand.append(");");
			System.out.println(sbCommand.toString());
			rs.MoveNext();
		}

	}

	public static Recordset getCommand(Connection c, String query) {
		Command comm = new Command();
		comm.setActiveConnection(c);
		comm.setCommandType(CommandTypeEnum.adCmdText);
		comm.setCommandText(query);
		return comm.Execute();

	}

	public static void generateInsertFavorecidos(Connection c, String command) {
		Recordset rs = getCommand(c, command);
		printFavorecido(rs);
	}

	
	public static void generateInsertObreiros(Connection c, String command) {
		Recordset rs = getCommand(c, command);
		printObreiro(rs);
	}

	public static void generateInsertConta(Connection c, String command) {
		Recordset rs = getCommand(c, command);
		printConta(rs);
	}

	private static void populaCPF(String nome, StringBuilder sb) {

		Map<String, String> dados = new HashMap<String, String>();
		try {
			File fileDir = new File("C:\\NET\\workspaces\\foxpro\\dados\\cpf.txt");
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF-8"));

			while (br.ready()) {
				String[] colunas = br.readLine().split(";");
				dados.put(colunas[1].trim(), colunas[0].trim().replaceAll("[^0-9]+", ""));
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		String cpf = dados.get(nome);
		if (cpf != null) {
			sb.append("'").append(cpf).append("'");
		} else {
			sb.append("''");
		}

	}

	public static void generateInsertCCObreiros(Connection c, String command) {
		Recordset rs = getCommand(c, command);
		printCCObreiro(rs);
	}

	public static void generateInsertCCRazao(Connection c, String command) {
		Recordset rs = getCommand(c, command);
		printCCRazao(rs);
	}

	public static void main(String[] args) {

		Connection c = new Connection();
		c.setConnectionString(connectStr);
		c.Open();

		generateInsertFavorecidos(c, "select * from lfa1");
		//generateInsertObreiros(c, "select * from cobr");
		//generateInsertConta(c, "select * from kont");
		//generateInsertCCObreiros(c, "select * from ccobr order by idmov");
		//generateInsertCCRazao(c, "select * from razao order by idmov");

		c.Close();

	}
}
