package no.ssb.dapla.spark.plugin.pseudo;

import no.ssb.dapla.spark.plugin.pseudo.PseudoFuncConfigFactory.FuncDeclaration;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PseudoFuncDeclarationTest {

    @Test
    public void fromString_withArguments_shouldParseSuccessfully() {
        FuncDeclaration funcDecl = FuncDeclaration.fromString("      functionName(arg1, arg2,,  , arg5   ,)            ");
        assertThat(funcDecl.getFuncName()).isEqualTo("functionName");
        assertThat(funcDecl.getArgs().size()).isEqualTo(6);
        assertThat(funcDecl.getArgs().get(0)).isEqualTo("arg1");
        assertThat(funcDecl.getArgs().get(1)).isEqualTo("arg2");
        assertThat(funcDecl.getArgs().get(2)).isEqualTo("");
        assertThat(funcDecl.getArgs().get(3)).isEqualTo("");
        assertThat(funcDecl.getArgs().get(4)).isEqualTo("arg5");
        assertThat(funcDecl.getArgs().get(5)).isEqualTo("");
        assertThat(funcDecl.toString()).isEqualTo("functionName(arg1,arg2,,,arg5,)");
    }

    @Test
    public void fromString_withArgumentsButMissingParentheses_shouldParseSuccessfully() {
        FuncDeclaration funcDecl = FuncDeclaration.fromString("functionName arg1, arg2,,  , arg5   ,");
        assertThat(funcDecl.getFuncName()).isEqualTo("functionName");
        assertThat(funcDecl.getArgs().size()).isEqualTo(6);
        assertThat(funcDecl.getArgs().get(0)).isEqualTo("arg1");
        assertThat(funcDecl.getArgs().get(1)).isEqualTo("arg2");
        assertThat(funcDecl.getArgs().get(2)).isEqualTo("");
        assertThat(funcDecl.getArgs().get(3)).isEqualTo("");
        assertThat(funcDecl.getArgs().get(4)).isEqualTo("arg5");
        assertThat(funcDecl.getArgs().get(5)).isEqualTo("");
        assertThat(funcDecl.toString()).isEqualTo("functionName(arg1,arg2,,,arg5,)");
    }

    @Test
    public void fromString_withoutArgumentsEmptyParentheses_shouldParseSuccessfully() {
        FuncDeclaration funcDecl = FuncDeclaration.fromString("    functionName()   ");
        assertThat(funcDecl.getFuncName()).isEqualTo("functionName");
        assertThat(funcDecl.getArgs().size()).isEqualTo(0);
        assertThat(funcDecl.toString()).isEqualTo("functionName()");
    }

    @Test
    public void fromString_withoutArgumentsNoParentheses_shouldParseSuccessfully() {
        FuncDeclaration funcDecl = FuncDeclaration.fromString("  functionName    ");
        assertThat(funcDecl.getFuncName()).isEqualTo("functionName");
        assertThat(funcDecl.getArgs().size()).isEqualTo(0);
        assertThat(funcDecl.toString()).isEqualTo("functionName()");
    }

    @Test
    public void fromString_withEmptyArguments_shouldParseSuccessfully() {
        FuncDeclaration funcDecl = FuncDeclaration.fromString("  functionName (,)    ");
        assertThat(funcDecl.getFuncName()).isEqualTo("functionName");
        assertThat(funcDecl.getArgs().size()).isEqualTo(2);
        assertThat(funcDecl.getArgs().get(0)).isEqualTo("");
        assertThat(funcDecl.getArgs().get(1)).isEqualTo("");
        assertThat(funcDecl.toString()).isEqualTo("functionName(,)");
    }
}