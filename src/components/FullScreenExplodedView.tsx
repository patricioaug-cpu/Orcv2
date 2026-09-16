import React, { useState, useMemo, useEffect } from "react";
import { CemigMaterialItem } from "../types";
import { resolveOfficialMaterialCode } from "../itemCatalogLookup";
import { getEstimatedMarketPrice } from "../cemigDatabase";
import {
  ArrowLeft,
  DollarSign,
  Eye,
  EyeOff,
  Printer,
  Minimize2,
  Check,
  X,
  Edit2,
  Trash2,
  FileText,
} from "lucide-react";

interface FullScreenExplodedViewProps {
  isOpen: boolean;
  onClose: () => void;
  materials: CemigMaterialItem[];
  effectiveTotalUS: number;
  effectiveTotalLaborValue?: number;
  profitMargin?: number;
  voltageLevel?: string;
  projectName?: string;
  onUpdateMaterial?: (updated: CemigMaterialItem[]) => void;
  onOpenRegisteredPrices?: () => void;
}

export const FullScreenExplodedView: React.FC<FullScreenExplodedViewProps> = ({
  isOpen,
  onClose,
  materials,
  profitMargin = 0,
  projectName,
  onUpdateMaterial,
}) => {
  const [showPrices, setShowPrices] = useState<boolean>(true);

  // Inline editing state
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editCode, setEditCode] = useState<string>("");
  const [editDesc, setEditDesc] = useState<string>("");
  const [editQty, setEditQty] = useState<number>(1);
  const [editUnitPrice, setEditUnitPrice] = useState<number>(0);

  // Keyboard shortcut: Escape to close
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape" && isOpen) {
        onClose();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [isOpen, onClose]);

  // Lock body scroll while in full screen
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => {
      document.body.style.overflow = "";
    };
  }, [isOpen]);

  // Filter materials (excluding removal items for consolidated install list)
  const installMaterials = useMemo(() => {
    return materials.filter(
      (item) => item.status !== "RETIRAR" && !item.description.startsWith("[A RETIRAR]")
    );
  }, [materials]);

  // Margin calculation helpers
  const activeMargin = Math.max(0, Number(profitMargin) || 0);

  const getBaseItemPrice = (item: CemigMaterialItem): number => {
    if (item.unitPrice !== undefined && item.unitPrice > 0) return item.unitPrice;
    return getEstimatedMarketPrice(item.code, item.category, item.description);
  };

  const getItemPrice = (item: CemigMaterialItem): number => {
    const base = getBaseItemPrice(item);
    if (activeMargin > 0) {
      return base * (1 + activeMargin / 100);
    }
    return base;
  };

  // Grand totals across all consolidated materials
  const totalBaseValue = useMemo(() => {
    return installMaterials.reduce((acc, item) => acc + item.quantity * getBaseItemPrice(item), 0);
  }, [installMaterials]);

  const totalEffectiveValue = useMemo(() => {
    return installMaterials.reduce((acc, item) => acc + item.quantity * getItemPrice(item), 0);
  }, [installMaterials, activeMargin]);

  const totalProfitValue = totalEffectiveValue - totalBaseValue;

  const formatCurrency = (val: number) => {
    return (Number(val) || 0).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
  };

  // Inline editing handlers
  const handleStartEdit = (item: CemigMaterialItem) => {
    setEditingId(item.id);
    setEditCode(item.code || "");
    setEditDesc(item.description);
    setEditQty(item.quantity);
    setEditUnitPrice(getItemPrice(item));
  };

  const handleSaveEdit = (id: string) => {
    if (!onUpdateMaterial) return;
    const updated = materials.map((item) => {
      if (item.id === id) {
        return {
          ...item,
          code: editCode.trim() || item.code,
          description: editDesc.trim() || item.description,
          quantity: editQty > 0 ? editQty : 1,
          unitPrice: editUnitPrice > 0 ? editUnitPrice : item.unitPrice,
        };
      }
      return item;
    });
    onUpdateMaterial(updated);
    setEditingId(null);
  };

  const handleDeleteItem = (id: string) => {
    if (!onUpdateMaterial) return;
    if (window.confirm("Deseja realmente remover este material da lista consolidada?")) {
      const updated = materials.filter((item) => item.id !== id);
      onUpdateMaterial(updated);
      if (editingId === id) setEditingId(null);
    }
  };

  if (!isOpen) return null;

  return (
    <div
      id="fullscreen-exploded-materials"
      className="fixed inset-0 z-50 bg-slate-900 text-slate-100 flex flex-col h-screen w-screen overflow-hidden animate-in fade-in duration-200"
    >
      {/* 1. TOP COMMAND BAR (Clean and minimal) */}
      <header className="bg-slate-950 border-b border-slate-800 px-3 sm:px-5 py-2.5 shrink-0 flex items-center justify-between gap-3 shadow-lg z-20 print:hidden">
        {/* Left: Back Button */}
        <button
          id="btn-back-to-main-screen"
          onClick={onClose}
          className="inline-flex items-center gap-2 px-3.5 py-2 bg-slate-800 hover:bg-slate-700 active:scale-95 text-white rounded-xl font-bold text-xs sm:text-sm transition-all border border-slate-700 shadow-sm cursor-pointer shrink-0"
          title="Voltar para a Tela Principal do Aplicativo (ou pressione Esc)"
        >
          <ArrowLeft className="w-4 h-4 text-amber-400 shrink-0" />
          <span>Voltar pra Tela Principal</span>
        </button>

        {/* Right: Actions */}
        <div className="flex items-center gap-2">
          {/* Toggle Prices Button */}
          <button
            onClick={() => setShowPrices(!showPrices)}
            className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-bold transition-all border cursor-pointer ${
              showPrices
                ? "bg-amber-500/20 text-amber-300 border-amber-500/50"
                : "bg-slate-800 text-slate-400 border-slate-700 hover:bg-slate-700"
            }`}
            title="Exibir ou ocultar colunas de valores em R$"
          >
            {showPrices ? <Eye className="w-3.5 h-3.5 text-amber-400" /> : <EyeOff className="w-3.5 h-3.5" />}
            <span className="hidden sm:inline">{showPrices ? "Valores R$: Exibidos" : "Valores R$: Ocultos"}</span>
          </button>

          {/* Print Button */}
          <button
            onClick={() => window.print()}
            className="inline-flex items-center gap-1.5 px-3.5 py-1.5 bg-slate-800 hover:bg-slate-700 active:scale-95 text-slate-200 border border-slate-700 rounded-xl text-xs font-bold transition-all cursor-pointer"
            title="Imprimir Relatório de Materiais ou Salvar como PDF"
          >
            <Printer className="w-3.5 h-3.5 text-amber-400" />
            <span>Imprimir</span>
          </button>

          {/* Close Button */}
          <button
            onClick={onClose}
            className="p-2 text-slate-400 hover:text-white bg-slate-800/60 hover:bg-slate-800 rounded-xl transition-all cursor-pointer border border-slate-700/60"
            title="Fechar Tela Cheia (Esc)"
          >
            <Minimize2 className="w-4 h-4" />
          </button>
        </div>
      </header>

      {/* 2. MAIN CONTENT CANVAS: Direct Consolidated Materials Sheet */}
      <main className="flex-1 overflow-y-auto p-2.5 sm:p-6 bg-slate-900/60 space-y-6 print:p-0 print:bg-white print:overflow-visible">
        {installMaterials.length === 0 ? (
          <div className="max-w-xl mx-auto my-16 bg-slate-800/80 border border-slate-700 rounded-2xl p-8 text-center space-y-3 shadow-xl">
            <FileText className="w-12 h-12 text-slate-500 mx-auto" />
            <h3 className="text-base font-bold text-white">Nenhum material consolidado encontrado</h3>
            <p className="text-xs text-slate-400">
              Retorne à tela principal para carregar ou processar o projeto técnico.
            </p>
            <button
              onClick={onClose}
              className="mt-2 inline-flex items-center gap-2 px-4 py-2 bg-amber-500 hover:bg-amber-400 text-slate-950 font-bold text-xs rounded-xl transition-all cursor-pointer"
            >
              <ArrowLeft className="w-4 h-4" />
              <span>Voltar pra Tela Principal</span>
            </button>
          </div>
        ) : (
          <div className="max-w-6xl mx-auto bg-white text-slate-900 rounded-2xl shadow-2xl border border-slate-300 overflow-hidden flex flex-col print:border-none print:shadow-none print:rounded-none">
            {/* Sheet Formal Header (Clean without US or pagination badges) */}
            <div className="bg-slate-950 text-white p-3.5 sm:p-4 border-b border-slate-800 flex items-center justify-between gap-3 flex-wrap">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-amber-500 text-slate-950 rounded-xl font-black shrink-0">
                  <FileText className="w-5 h-5" />
                </div>
                <div>
                  <h2 className="text-sm sm:text-base font-extrabold text-white">
                    Lista de Materiais de Distribuição Consolidada
                  </h2>
                  {projectName && (
                    <p className="text-[11px] text-slate-400 truncate max-w-lg mt-0.5">
                      Projeto: {projectName}
                    </p>
                  )}
                </div>
              </div>

              {/* Total items badge */}
              <div className="bg-slate-900 border border-slate-800 px-3 py-1.5 rounded-xl text-right">
                <span className="text-[10px] text-slate-400 block uppercase font-bold">Total de Itens</span>
                <span className="font-mono font-bold text-white text-xs">
                  {installMaterials.length} materiais
                </span>
              </div>
            </div>

            {/* Table of Consolidated Materials */}
            <div className="overflow-x-auto">
              <table className="w-full text-left border-collapse text-xs">
                <thead>
                  <tr className="bg-slate-100 text-slate-700 border-b border-slate-300 font-bold uppercase tracking-wider text-[11px]">
                    <th className="py-3 px-3.5 w-12 text-center">#</th>
                    <th className="py-3 px-3.5 w-32">Código</th>
                    <th className="py-3 px-3.5">Descrição do Material</th>
                    <th className="py-3 px-3.5 w-16 text-center">Unid.</th>
                    <th className="py-3 px-3.5 w-24 text-right">Qtd.</th>
                    {showPrices && (
                      <>
                        <th className="py-3 px-3.5 w-32 text-right">Val. Unit. (R$)</th>
                        <th className="py-3 px-3.5 w-36 text-right">Val. Total (R$)</th>
                      </>
                    )}
                    <th className="py-3 px-3.5 w-24 text-center print:hidden">Ações</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-200">
                  {installMaterials.map((item, idx) => {
                    const isEditing = editingId === item.id;
                    const price = getItemPrice(item);
                    const itemTotal = item.quantity * price;

                    return (
                      <tr
                        key={`full_mat_${item.id || "elem"}_${idx}`}
                        className={`hover:bg-slate-50 transition-colors ${
                          isEditing ? "bg-amber-50/60" : idx % 2 === 0 ? "bg-white" : "bg-slate-50/30"
                        }`}
                      >
                        <td className="py-2.5 px-3.5 text-center text-slate-400 font-mono">
                          {idx + 1}
                        </td>

                        {/* Code */}
                        <td className="py-2.5 px-3.5 font-mono font-bold text-slate-900">
                          {isEditing ? (
                            <input
                              type="text"
                              value={editCode}
                              onChange={(e) => setEditCode(e.target.value)}
                              className="w-full px-2 py-1 text-xs border border-amber-400 rounded-lg focus:outline-none bg-white font-mono"
                            />
                          ) : (
                            <span className="bg-slate-100 text-slate-900 px-2 py-0.5 rounded border border-slate-200 inline-block">
                              {resolveOfficialMaterialCode(item)}
                            </span>
                          )}
                        </td>

                        {/* Description */}
                        <td className="py-2.5 px-3.5 text-slate-900">
                          {isEditing ? (
                            <input
                              type="text"
                              value={editDesc}
                              onChange={(e) => setEditDesc(e.target.value)}
                              className="w-full px-2 py-1 text-xs border border-amber-400 rounded-lg focus:outline-none bg-white"
                            />
                          ) : (
                            <div>
                              <span className="font-semibold text-slate-900">{item.description}</span>
                              {item.sourceStructureName && (
                                <span className="ml-2 text-[10px] font-bold text-amber-800 bg-amber-100 px-1.5 py-0.5 rounded font-mono">
                                  {item.sourceStructureName}
                                </span>
                              )}
                            </div>
                          )}
                        </td>

                        {/* Unit */}
                        <td className="py-2.5 px-3.5 text-center text-slate-700 font-bold">
                          {item.unit}
                        </td>

                        {/* Quantity */}
                        <td className="py-2.5 px-3.5 text-right font-black text-slate-900 font-mono">
                          {isEditing ? (
                            <input
                              type="number"
                              step="any"
                              value={editQty}
                              onChange={(e) => setEditQty(Number(e.target.value))}
                              className="w-20 px-2 py-1 text-xs border border-amber-400 rounded-lg text-right focus:outline-none bg-white font-mono font-bold"
                            />
                          ) : (
                            (Number(item.quantity) || 0).toLocaleString("pt-BR")
                          )}
                        </td>

                        {/* Price Columns */}
                        {showPrices && (
                          <>
                            <td className="py-2.5 px-3.5 text-right font-mono text-slate-800">
                              {isEditing ? (
                                <input
                                  type="number"
                                  step="0.01"
                                  value={editUnitPrice}
                                  onChange={(e) => setEditUnitPrice(Number(e.target.value))}
                                  className="w-24 px-2 py-1 text-xs border border-amber-400 rounded-lg text-right focus:outline-none bg-white font-mono font-bold"
                                />
                              ) : price > 0 ? (
                                formatCurrency(price)
                              ) : (
                                "R$ 0,00"
                              )}
                            </td>
                            <td className="py-2.5 px-3.5 text-right font-mono font-bold text-slate-950">
                              {itemTotal > 0 ? formatCurrency(itemTotal) : "R$ 0,00"}
                            </td>
                          </>
                        )}

                        {/* Actions */}
                        <td className="py-2.5 px-3.5 text-center print:hidden">
                          {isEditing ? (
                            <div className="flex items-center justify-center gap-1">
                              <button
                                onClick={() => handleSaveEdit(item.id)}
                                className="p-1 text-emerald-700 bg-emerald-100 hover:bg-emerald-200 rounded cursor-pointer"
                                title="Salvar"
                              >
                                <Check className="w-3.5 h-3.5" />
                              </button>
                              <button
                                onClick={() => setEditingId(null)}
                                className="p-1 text-rose-700 bg-rose-100 hover:bg-rose-200 rounded cursor-pointer"
                                title="Cancelar"
                              >
                                <X className="w-3.5 h-3.5" />
                              </button>
                            </div>
                          ) : (
                            <div className="flex items-center justify-center gap-1">
                              <button
                                onClick={() => handleStartEdit(item)}
                                className="p-1 text-slate-500 hover:text-slate-900 bg-slate-100 hover:bg-slate-200 rounded cursor-pointer"
                                title="Editar"
                              >
                                <Edit2 className="w-3 h-3" />
                              </button>
                              <button
                                onClick={() => handleDeleteItem(item.id)}
                                className="p-1 text-rose-500 hover:text-rose-700 bg-rose-50 hover:bg-rose-100 rounded cursor-pointer"
                                title="Remover"
                              >
                                <Trash2 className="w-3 h-3" />
                              </button>
                            </div>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {/* SHEET FOOTER / TOTALIZER LOGIC */}
            <div className="p-4 sm:p-5 bg-slate-950 text-white border-t-2 border-amber-500 flex flex-col md:flex-row md:items-center justify-between gap-4">
              <div className="flex items-center gap-4">
                <div className="p-3 bg-amber-500 text-slate-950 rounded-2xl shrink-0 shadow-lg">
                  <DollarSign className="w-6 h-6 sm:w-7 sm:h-7" />
                </div>
                <div>
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-xs font-bold text-amber-400 uppercase tracking-wider">
                      VALOR TOTAL DOS MATERIAIS CONSOLIDADOS
                    </span>
                    {activeMargin > 0 && (
                      <span className="px-2 py-0.5 text-[10px] font-black bg-amber-500 text-slate-950 rounded-md">
                        +{activeMargin}% MARGEM APLICADA
                      </span>
                    )}
                  </div>
                  {showPrices ? (
                    <div className="mt-1">
                      <span className="text-2xl sm:text-3xl font-black text-white font-mono tracking-tight">
                        {formatCurrency(totalEffectiveValue)}
                      </span>
                      {activeMargin > 0 && (
                        <div className="text-xs text-slate-400 flex items-center gap-3 mt-1 font-mono">
                          <span>Custo Base: {formatCurrency(totalBaseValue)}</span>
                          <span>•</span>
                          <span className="text-emerald-400 font-bold">Lucro: +{formatCurrency(totalProfitValue)}</span>
                        </div>
                      )}
                    </div>
                  ) : (
                    <span className="text-sm italic text-slate-400 mt-1 block">
                      Valores ocultos (habilite em "Valores R$: Exibidos")
                    </span>
                  )}
                </div>
              </div>

              {/* Close / Back Action */}
              <div className="flex items-center gap-3 justify-end">
                <button
                  onClick={onClose}
                  className="px-4 py-2.5 bg-amber-500 hover:bg-amber-400 active:scale-95 text-slate-950 font-black rounded-xl transition-all cursor-pointer shadow-md text-xs sm:text-sm flex items-center gap-2"
                >
                  <ArrowLeft className="w-4 h-4" />
                  <span>Voltar pra Tela Principal</span>
                </button>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
};
